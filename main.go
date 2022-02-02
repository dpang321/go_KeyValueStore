package main

// standard imports
import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// message struct is used to unpack request vals into a struct that can handle null causal metadata
type message struct {
	Value          interface{}  `json:"value"`
	CausalMetadata *ReqMetaData `json:"causal-metadata"`
}

// reqMetaData is used to unpack request vals when they actually exist and are not null so they can be easily assigned a type
type ReqMetaData struct {
	ReqVector       []int `json:"ReqVector, omitempty"`
	ReqIpIndex      int   `json:"ReqIpIndex, omitempty"`
	IsReqFromClient bool  `json:"IsReqFromClient, omitempty"`
}

// declaring our Vector Clock, which we'll use for causal consistency
type VectorClock struct {
	VC []int `json:"VC, omitempty"`
}

var replicaArray []string // holds IP's of all replicas
var replicaCount = 0      // local Counter for number of replicas online
var sAddress string       // socket address
var viewArray []string    // array of IP's currently in view i.e. online
var vectorIndex = -1      // represents which index in replicaArray this current thread is
var shardCount = -1       // represents # of shards we are given at start of program

var shardSplit = make(map[string][]string) // map of shard names to an array of the IP's in that shard
var ipToShardMap = make(map[string]string) // map for use when switching between an IP and a shard
var hashIndexArr []string                  // array of all shard names
var currentShard string                    // indicates which shard this instance is a part of

// first 3 integers represent the vector clock of the local replica
// 4th vector is the index of the Ip that all replicas have access to
// 5th vector is the binary toggle (1 or 0) that determines if the data was from a client (1) or a replica (0)
var localVector []int

// Used in mapping replica IP to index of the replicaArray
var ipToIndex = make(map[string]int)

// our local KVS store
var store = make(map[string]interface{})

func main() {
	r := mux.NewRouter()

	//testing purposes
	//os.Setenv("SOCKET_ADDRESS", "10.10.0.2:8090")
	//os.Setenv("VIEW", "10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090")

	//pulls unique replica address from env variable
	sAddress = os.Getenv("SOCKET_ADDRESS")

	// grabbing env variables that are passed in
	vAddresses := os.Getenv("VIEW")
	shardCountString := os.Getenv("SHARD_COUNT")

	// converting shardCount to an integer from the env variable
	if shardCountString != "" {
		shardCountOutput, err := strconv.Atoi(shardCountString)
		if err != nil {
			fmt.Println("error converting string to int = ", err)
		}
		shardCount = shardCountOutput
	}

	// splitting the arrays by comma delimiter
	replicaArray = strings.Split(vAddresses, ",")
	viewArray = strings.Split(vAddresses, ",")

	// creating the local vectors
	for index, ip := range viewArray {
		ipToIndex[ip] = index
		localVector = append(localVector, 0)
		if ip == sAddress {
			vectorIndex = index
		}
	}

	// debug
	fmt.Println("ip to index == ", ipToIndex)
	fmt.Println("vector index ===", vectorIndex)
	fmt.Println("localVector === ", localVector)

	// Handlers for each scenario of input for URL
	r.HandleFunc("/view", handleView)
	r.HandleFunc("/kvs/{key}", handleKey)

	// helper functions for communication between nodes/replicas
	r.HandleFunc("/getVC", handleGetVC)
	r.HandleFunc("/setVC", handleSetVC)
	r.HandleFunc("/getKVS", handleGetKVS)
	r.HandleFunc("/getShardSplit", handleGetShardSplit)
	r.HandleFunc("/resplitNodes", handleResplitNodes)

	// Handlers for sharding requests
	r.HandleFunc("/shard/ids", handleShardAllId)
	r.HandleFunc("/shard/node-shard-id", handleShardOneId)
	r.HandleFunc("/shard/members/{id}", handleShardMembers)
	r.HandleFunc("/shard/key-count/{id}", handleShardKeyCount)
	r.HandleFunc("/shard/add-member/{id}", handleShardAddMember)
	r.HandleFunc("/shard/reshard", handleReshard)
	r.HandleFunc("/shard/broadcast/add/{id}", handleBroadcastedAdd)

	// function that checks if this replica has just died
	go didIDie()

	// splitting all replica IP's into shards
	if shardCount != -1 {
		splitNodes(shardCount)
		currentShard = ipToShardMap[sAddress]
	}
	fmt.Println("current shard = ", currentShard)

	// Service listens on port 8090
	log.Fatal(http.ListenAndServe(":8090", r))
}

// sending a request to a replicaIP to get its shard split
func getShardSplit(replicaIP string) map[string][]string {
	var response map[string]map[string][]string

	// Creating new request
	fmt.Println("replicaIP ==== ", replicaIP)
	res, err := http.Get(fmt.Sprintf("http://%s/getShardSplit", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request here")
		log.Fatalf("Error: %s", err)
	}

	// decoding the response of new request
	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}
	responseShardSplit := response["shard-split"]
	// returning the VC from other replica
	return responseShardSplit
}

// Helper function to forward requests between our replicas
func forwardReq(rMethod string, rIP string, rPath string, rBody []byte) ([]byte, int) {
	client := &http.Client{}
	// Creating new request
	req, err := http.NewRequest(rMethod, fmt.Sprintf("http://%s%s", rIP, rPath), bytes.NewBuffer(rBody))
	if err != nil {
		log.Fatalf("problem creating new http request: %s", err)
	}
	// Forwarding the new request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("%s is down due to: %s", rIP, err)
	}
	// Closing body of resp, typical after using Client.do()
	//defer resp.Body.Close()
	if resp != nil {
		returnBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		return returnBytes, resp.StatusCode
	}
	return make([]byte, 0), http.StatusBadRequest
}

// comparing two slices of an array and seeing if they're equal
func compareSlices(s1 []int, s2 []int) bool {
	for i, v := range s2 {
		if v != s1[i] {
			return false
		}
	}
	return true
}

// helper function that will hash a key, nodulo by shardCount, returns a number corresponding to which
// shard the key will hash into
func hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % shardCount
}

// helper function that sends a request to a shard that will return the number of keys in that shard
func getShardKeyCount(ipInShard string, shardId string) []byte {
	res, err := http.Get(fmt.Sprintf("http://%s/shard/key-count/%s", ipInShard, shardId))
	if err != nil {
		fmt.Println("problem creating new http request")
	}
	fmt.Println(shardId, "outside", currentShard, "scope. grabbing key count from", ipInShard)
	// decoding the response of new request
	returnBytes, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	return returnBytes
}

// helper function that will assign replica IP's to a shard
func splitNodes(shardAmount int) {
	// declaration of necessary variables
	shardSplitArray := make([][]string, 0)
	shardSplit = make(map[string][]string)
	ipToShardMap = make(map[string]string)
	store = make(map[string]interface{})

	shardCount = shardAmount
	hashIndexArr = nil

	// appending new string arrays to shardSplitArray
	for i := 0; i < shardAmount; i++ {
		shardSplitArray = append(shardSplitArray, make([]string, 0))
	}

	// adding replica IP's to a shard based on their index and a modulo by shardcount
	for i := 0; i < len(replicaArray); i++ {
		x := i % shardAmount
		shardSplitArray[x] = append(shardSplitArray[x], replicaArray[i])
		ipToShardMap[replicaArray[i]] = "s" + strconv.Itoa(x)
	}

	// adding shard names, i.e. s0 s1 s2
	for i := 0; i < shardCount; i++ {
		shardName := "s" + strconv.Itoa(i)
		shardSplit[shardName] = shardSplitArray[i]
		hashIndexArr = append(hashIndexArr, shardName)
	}

	// debug
	fmt.Println("shardSplitArray ===", shardSplitArray)
	fmt.Println("shardSplit ===", shardSplit)
	fmt.Println("hashIndex arr", hashIndexArr)
	fmt.Println("\n\n ip to shard mapping === \n", ipToShardMap)

}

// Used to check if current replica has just died
func didIDie() {
	// sleep for a second, so that we can confirm other replicas have time to start up
	time.Sleep(time.Second * 4) //tests work with 4
	// checking all elements of current view
	for _, replicaIP := range replicaArray {
		// if any in the view is not our address
		if replicaIP != sAddress {
			// gets vector clock of that other replica
			var repVC = getReplicaVectorClock(replicaIP)
			fmt.Println("repVC === ", repVC)

			// if other replica's VC is not equal to our own
			if !compareSlices(repVC, localVector) {
				//set local VC to grabbed VC
				localVector = repVC
				shardSplit = getShardSplit(replicaIP)
				shardCount = len(shardSplit)

				//we know we died and need to grab the new KVS
				//store = getReplicaKVS(replicaIP)
				//and push our Ip to the replica Array
				pushToView(sAddress, "socket-address", "view")
				return
			}
		}
	}
}

// Function used to send our IP to a replica's view array
func pushToView(responseVal interface{}, responseName string, URLPath string) {
	// making a response, and map our socket address
	response := make(map[string]interface{})
	response[responseName] = responseVal

	// standard json marshal of repsonse
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}

	// checking each replica IP of all replicas
	for _, replicaIP := range replicaArray {
		// if the replica IP is not our own
		if replicaIP != sAddress {
			client := &http.Client{}
			// Creating new request to PUT our IP in the replica's view array
			req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s/%s", replicaIP, URLPath), bytes.NewBuffer(jsonResponse))
			if err != nil {
				fmt.Println("problem creating new http request")
			}

			// Forwarding the new request
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println(replicaIP, " is down due to: ", err)
				return
			}
			defer resp.Body.Close()
			fmt.Printf("push to %s gave response %v \n", replicaIP, resp.StatusCode)
		}
	}
}

// Function used to get the kvs store of another replica
func getReplicaKVS(replicaIP string) map[string]interface{} {
	var response map[string]interface{}

	// Creating new request
	res, err := http.Get(fmt.Sprintf("http://%s/getKVS", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request")
	}

	// decoding the response of new request
	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}

	// returning the KVS
	return response["KVS"].(map[string]interface{})
}

// Function used  to get the vector clock of another replica
func getReplicaVectorClock(replicaIP string) []int {
	var response VectorClock

	// Creating new request
	fmt.Println("replicaIP ==== ", replicaIP)
	res, err := http.Get(fmt.Sprintf("http://%s/getVC", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request here")
		return make([]int, len(replicaArray))
	}

	// decoding the response of new request
	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}

	// returning the VC from other replica
	return response.VC
}

// Helper function used to check if the database has been changed
func isDatabaseChanged(response map[string]interface{}) bool {

	//check if a value actually got added to db, and if so we need to alert the other replicas
	if _, ok := response["result"]; ok {
		val := response["result"]
		if val == "created" || val == "updated" || val == "deleted" {
			return true
		}
	}
	return false
}

// Helper function used to broadcast a message to a replica
func broadcastMessage(replicaIP string, req *http.Request, updatedBody []byte) {

	client := &http.Client{}
	fmt.Println("req method: ", req.Method)
	fmt.Println("req URL: ", fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path))

	// Creating new request
	req, err := http.NewRequest(req.Method, fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path), bytes.NewBuffer(updatedBody))
	if err != nil {
		fmt.Println("problem creating new http request")
		return
	}

	// Checking to see if we can even reach that IP
	reachedURL, err := net.DialTimeout("tcp", replicaIP, (1 * time.Second))
	// If we can't reach that Replica, we know that it is down
	// Therefore, we must then broadcast and update views of all replicas
	if err != nil {
		fmt.Println(replicaIP, " is down! didnt reply within 2 seconds")

		// finding the index of the replica in array of online replicas
		i := containsVal(replicaIP, replicaArray)
		if i >= 0 {
			// removing that index from the array of online replicas
			replicaArray = removeVal(i, replicaArray)
		}
		fmt.Println("view is now ===", replicaArray)

		// Looping thru all replica IPs in replicaArray
		for _, repIP := range replicaArray {
			// if that replica IP is not the one we are broadcasting to, and is not our current replica
			if repIP != replicaIP && repIP != sAddress {
				fmt.Println("tell ", repIP, "that ", replicaIP, " is down")

				// making a variable to hold replicaIP
				viewBody := map[string]string{
					"socket-address": replicaIP,
				}

				// standard json marshalling
				viewBodyJson, err := json.Marshal(viewBody)
				if err != nil {
					log.Fatalf("Error: %s", err)
				}

				// checking if repIP is reachable
				reachedURL, err := net.DialTimeout("tcp", repIP, (2 * time.Second))
				if err != nil {
					fmt.Println("couldnt reach for DELETE", repIP, "err ===", err)
					return
				}

				// Creating new delete request to deletethe down replica IP from a replica's view
				reachedURL.Close()
				viewReq, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/view", repIP), bytes.NewBuffer(viewBodyJson))
				if err != nil {
					fmt.Println("Error broadcasting view: delete to replicas")
					return
				}

				// Sending delete request to the replica
				res, err := client.Do(viewReq)
				if err != nil {
					fmt.Println("problem creating new http request for view delete broadcast")
					return
				}
				defer res.Body.Close()
				//fmt.Println("delete body == ", res.Body)
			}
		}
		//check if system needs to be resharded
		shardDist := float64(len(replicaArray)) / float64(shardCount)
		if shardDist < 2.0 {
			shardResponse := make(map[string]float64)
			newShardCount := math.Floor(shardDist)
			shardResponse["shard-count"] = newShardCount
			shardBodyJson, err := json.Marshal(shardResponse)
			if err != nil {
				log.Fatalf("Error: %s", err)
			}
			client := &http.Client{}
			req, err := http.NewRequest(req.Method, fmt.Sprintf("http://%s/shard/reshard", sAddress), bytes.NewBuffer(shardBodyJson))
			if err != nil {
				fmt.Println("problem creating new http request")
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println(replicaIP, " is down due to: ", err)
				return
			}
			resp.Body.Close()
		}
		return
	}
	reachedURL.Close()

	// Forwarding the new request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(replicaIP, " is down due to: ", err)
		return
	}
	// Closing body of resp, typical after using Client.do()
	defer resp.Body.Close()
	return
}

// Helper function to check if an array contains a certain value, and at what index
func containsVal(val string, repArray []string) int {
	for index, a := range repArray {
		if a == val {
			return index
		}
	}
	return -1
}

// Helper function to remove a Value from a certain string array
func removeVal(index int, repArray []string) []string {
	repArray[index] = repArray[len(repArray)-1]
	return repArray[:len(repArray)-1]
}

// handler function to return the shard split for a request
func handleGetShardSplit(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["shard-split"] = shardSplit
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

// handler function that will add an IP to a shard
func handleBroadcastedAdd(w http.ResponseWriter, req *http.Request) {
	fmt.Println("broadcast recieved")

	// grabbing/assigning variables needed
	param := mux.Vars(req)
	id := param["id"]
	var reqVals map[string]string

	// decode request
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}

	// appending correct ip's to the shard
	shardSplit[id] = append(shardSplit[id], reqVals["add-ip"])
	if reqVals["add-ip"] == sAddress {
		currentShard = id
		store = getReplicaKVS(shardSplit[id][0])
	}

	fmt.Println("shard split updated to: ", shardSplit)
	w.WriteHeader(http.StatusOK)
}

// handler function that will set local VC to a value
func handleSetVC(w http.ResponseWriter, req *http.Request) {
	// assigning variables needed
	var reqVals map[string][]int
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}

	// reassign local VC
	localVector = reqVals["VC"]
	fmt.Println("local vector now set to", localVector)
	w.WriteHeader(http.StatusOK)
}

// Handler Function that handles when we are given a request to return
// our local VC, which we send out as a JSON object
func handleGetVC(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["VC"] = localVector
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

// Handler functuon that handles when we are guven a request to return
// our local KVS, which we send out as json object
func handleGetKVS(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["KVS"] = store
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

// Handler function that handles receiving a shardSplit from a request's body
// Used in accomplishing handleReshard's goal of having every other replica be aware
// of the new shards after resharding
func handleResplitNodes(w http.ResponseWriter, req *http.Request) {
	// create dict variable to hold inputted value
	var reqVals map[string]interface{}

	if req.Method == "PUT" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&reqVals)
		if err != nil {
			log.Fatalf("Error couldnt decode: %s", err)
			return
		}
		fmt.Print("reqVals === ", reqVals)
		newStore := reqVals["shard-store"].(map[string]interface{})
		fmt.Print("new store ===", newStore)
		newShardCountFloat := reqVals["shard-count"].(float64)
		newShardCount := int(newShardCountFloat)
		// grabbing shardcount from request json body
		/*
			newShardCount, err := strconv.Atoi(reqVals["shard-count"].(string))
			if err != nil {
				fmt.Println("error converting string to int = ", err)
			}
		*/
		fmt.Print("newShardCount == ", newShardCount)
		splitNodes(newShardCount)
		store = newStore
		currentShard = ipToShardMap[sAddress]

		w.WriteHeader(http.StatusOK)
	}

}

// Handler function that handles all operations when we are  given
// requests regarding our KVS
func handleKey(w http.ResponseWriter, req *http.Request) {

	// grabbing params to be used
	param := mux.Vars(req)
	key := param["key"]

	// initilizations of necessary variables
	response := make(map[string]interface{})
	broadcastResponse := make(map[string]interface{})
	var responseMetadata ReqMetaData

	// create dict variable to hold inputted value
	var reqVals message

	fmt.Println("req.Body === \n", req.Body)

	// handles pulling out and storing value into newVal
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}

	//check which shard it belongs to
	hashedKeyIndex := hash(key)
	//if true key belongs to shard we are in, so handle accordingly
	//if false, key belongs to another shard, so forawr request to first ip in correct shard
	if hashIndexArr[hashedKeyIndex] == ipToShardMap[sAddress] {
		fmt.Println("hashing at: ", sAddress)

		// assigning metadata from our request
		metadata := reqVals.CausalMetadata

		fmt.Println("localvector on recieve === ", localVector)

		// If metadata is not empty, we  know that this is not first interaction with client
		if metadata != nil {
			reqVector := metadata.ReqVector
			responseMetadata.IsReqFromClient = metadata.IsReqFromClient
			fmt.Println("vector clock from request === ", reqVector)
			//fmt.Println("IP index === ", metadata.ReqIpIndex)
			//fmt.Println("req from client? === ", metadata.IsReqFromClient)

			//check for consistency violations
			for i := 0; i < len(reqVector); i++ {
				if metadata.IsReqFromClient {
					if reqVector[i] > localVector[i] {
						//consistency violation
						//fmt.Println("bigger in the ", i, " position")
						w.WriteHeader(http.StatusServiceUnavailable)
						response["error"] = "Causal dependencies not satisfied; try again later"
					}
				} else {
					if i == metadata.ReqIpIndex {
						if reqVector[i] != localVector[i]+1 {
							//consistency violation
							//fmt.Println("bigger in the metadata.repipindex position: ", reqVector[i], " != ", localVector[i]+1, " when i = ", i)
							w.WriteHeader(http.StatusServiceUnavailable)
							response["error"] = "Causal dependencies not satisfied; try again later"
						}
					} else if reqVector[i] > localVector[i] {
						//consistency violation
						//fmt.Println("bigger in the ", i, " position")
						w.WriteHeader(http.StatusServiceUnavailable)
						response["error"] = "Causal dependencies not satisfied; try again later"
					}
				}
			}

			// if no causal dependency is detected, set the local clock to the max of the local clock and request clock
			if _, violation := response["error"]; !violation {
				if req.Method != "GET" {
					for i := 0; i < len(reqVector); i++ {
						if reqVector[i] > localVector[i] {
							localVector[i] = reqVector[i]
						}
					}
				}
			}

		} else {
			//handling inital nil case all future client requests will increment local clock when getting the max
			responseMetadata.IsReqFromClient = true
		}

		if _, violation := response["error"]; !violation {

			// PUT case
			if req.Method == "PUT" {

				val := reqVals.Value
				// handling cases where user input is:
				// 1. invalid (key too long)
				// 2. invalid (no value specified)
				// 3. being replaced (key already exists)
				// 4. being created (key does not exist)
				if len(key) > 50 {
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "Key is too long"
				} else if val == nil {
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "PUT request does not specify a value"
				} else if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "updated"
					store[key] = val
				} else {
					w.WriteHeader(http.StatusCreated)
					response["result"] = "created"
					store[key] = val
				}

				// GET case
			} else if req.Method == "GET" {

				// handling cases where user input is:
				// 1. valid (key exists)
				// 2. invalid (key does not exist)
				if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "found"
					response["value"] = store[key]
				} else {
					w.WriteHeader(http.StatusNotFound)
					response["error"] = "Key does not exist"
				}

				// DELETE case
			} else if req.Method == "DELETE" {

				// handling cases where user input is;
				// 1. valid (key exists)
				// 2. invalid (key does not exist)
				if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "deleted"
					delete(store, key)
				} else {
					w.WriteHeader(http.StatusNotFound)
					response["error"] = "Key does not exist"
				}
			}

			// reassigning necessary values in our response metadata
			responseMetadata.ReqVector = localVector
			responseMetadata.ReqIpIndex = ipToIndex[sAddress]

			// checking if we changed our database, and if so, to increment VC
			if isDatabaseChanged(response) {
				// Incrementing if the request is from the client
				if responseMetadata.IsReqFromClient {
					localVector[vectorIndex]++
				}

				//update response to updated clock index
				responseMetadata.ReqVector = localVector

				//if from client we need to broadcast req to other replicas
				//if from replica, we dont do anything here
				if responseMetadata.IsReqFromClient {
					var broadcastMetadata ReqMetaData
					broadcastMetadata.ReqVector = localVector
					broadcastMetadata.ReqIpIndex = ipToIndex[sAddress]
					broadcastMetadata.IsReqFromClient = false
					broadcastResponse["value"] = reqVals.Value
					broadcastResponse["causal-metadata"] = broadcastMetadata
					updatedBody, err := json.Marshal(broadcastResponse)
					if err != nil {
						log.Fatalf("response not marshalled: %s", err)
						return
					}

					fmt.Println("updated body ===", string(updatedBody))

					//broadcast to other replicas
					for _, replicaIP := range shardSplit[currentShard] {
						if replicaIP != sAddress {
							//need to update so that we only broadcast to replicas inside our own shard
							broadcastMessage(replicaIP, req, updatedBody)
						}
					}
					pushToView(localVector, "VC", "setVC")
				}
			}

			//set responses metadata to updated metadata
			response["causal-metadata"] = responseMetadata
			response["shard-id"] = ipToShardMap[sAddress]
		}
		fmt.Println("localvector after request is processed === ", localVector)
		fmt.Println("view after kvs update === ", replicaArray)
		// sending correct response / status code back to client
		fmt.Println("response === ", response)
		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		w.Write(jsonResponse)
	} else {
		forwardRequest := make(map[string]interface{})
		forwardRequest["value"] = reqVals.Value
		forwardRequest["causal-metadata"] = reqVals.CausalMetadata
		marshalledRequest, err := json.Marshal(forwardRequest)
		if err != nil {
			log.Fatalf("couldnt marshall: %s", err)
		}
		shardToForwardRequest := hashIndexArr[hashedKeyIndex]
		replicaIP := shardSplit[shardToForwardRequest][0]

		fmt.Println("key hashed to ", shardToForwardRequest, " sending to ", replicaIP)
		jsonResponse, statusCode := forwardReq(req.Method, replicaIP, req.URL.Path, marshalledRequest)
		w.WriteHeader(statusCode)
		w.Write(jsonResponse)
	}
}

// Handler function that handles all program behavior regarding view operations
func handleView(w http.ResponseWriter, req *http.Request) {

	response := make(map[string]interface{})

	// create dict variable to hold inputted value
	var newVal map[string]interface{}

	if req.Method == "PUT" {

		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		valtemp := newVal["socket-address"]
		val := fmt.Sprintf("%v", valtemp)
		fmt.Println("val recieved === ", val)

		// auto adding if this is first replica
		if replicaCount == 0 {
			replicaArray = append(replicaArray, val)
			w.WriteHeader(http.StatusCreated)
			response["result"] = "added"
			replicaCount++
		} else {
			// checking to make sure entry is already  present
			if containsVal(val, replicaArray) >= 0 {
				w.WriteHeader(http.StatusOK)
				response["result"] = "already present"
			} else {
				replicaArray = append(replicaArray, val)
				w.WriteHeader(http.StatusCreated)
				response["result"] = "added"
				replicaCount++
			}
		}
	} else if req.Method == "GET" {
		// simply returning replica array for view
		w.WriteHeader(http.StatusOK)
		response["view"] = replicaArray

	} else if req.Method == "DELETE" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		valtemp := newVal["socket-address"]
		val := fmt.Sprintf("%v", valtemp)

		// finding index of the value in replica array
		index := containsVal(val, replicaArray)
		// if it is found
		if index >= 0 {
			// delete the replica from view
			replicaArray = removeVal(index, replicaArray)
			w.WriteHeader(http.StatusCreated)
			response["result"] = "deleted"
			replicaCount--
		} else {
			// returning when replica is not found
			w.WriteHeader(http.StatusNotFound)
			response["error"] = "View has no such replica"
		}
	}

	fmt.Println("view after view update === ", replicaArray)
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)
}

// Handler function that will handle return all shard ID's
func handleShardAllId(w http.ResponseWriter, req *http.Request) {
	// assigning variables needed
	allId := make([]string, 0)
	response := make(map[string]interface{})

	// if method is GET
	if req.Method == "GET" {
		// append shard ID's to response
		for key := range shardSplit {
			allId = append(allId, key)
		}
		w.WriteHeader(http.StatusOK)
		response["shard-ids"] = allId
	}

	// write response
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)

}

// Handler function that will return current shard given an IP
func handleShardOneId(w http.ResponseWriter, req *http.Request) {
	// assigning variables needed
	response := make(map[string]interface{})

	if req.Method == "GET" {
		// assigning reponse to return currennt shard id
		w.WriteHeader(http.StatusOK)
		response["node-shard-id"] = currentShard
	}

	// response
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)

}

// Handler function that will return all members of a shard
func handleShardMembers(w http.ResponseWriter, req *http.Request) {
	// assigning variables needed
	param := mux.Vars(req)
	id := param["id"]
	response := make(map[string]interface{})

	if req.Method == "GET" {
		// assigning all members of that shard, if the shard is valid
		if shardArray, ok := shardSplit[id]; ok {
			w.WriteHeader(http.StatusOK)
			response["shard-members"] = shardArray
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	// response
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)
}

// Handler function that will return the key count for a given shard
func handleShardKeyCount(w http.ResponseWriter, req *http.Request) {
	// assigning variables needed
	param := mux.Vars(req)
	id := param["id"]
	response := make(map[string]interface{})

	if req.Method == "GET" {
		// if we are asking for key count of current shard
		if currentShard == id {
			// returning length of kvs
			w.WriteHeader(http.StatusOK)
			response["shard-key-count"] = len(store)

			// response
			jsonResponse, err := json.Marshal(response)
			if err != nil {
				log.Fatalf("Error here: %s", err)
			}
			w.Write(jsonResponse)
		} else {
			//check if id is even a shard
			if _, ok := shardSplit[id]; !ok {
				w.WriteHeader(http.StatusNotFound)
			} else {
				//key isnt equal to id, meaning this replica is not a member of the shard we want the key count of
				//grab first ip thats a member of desired shard
				ipInShard := shardSplit[id][0]
				forwardResponse := getShardKeyCount(ipInShard, id)
				w.Write(forwardResponse)
			}
		}
	}
}

// Handler function that will add an IP to a shard
func handleShardAddMember(w http.ResponseWriter, req *http.Request) {
	// assigning variables needed
	param := mux.Vars(req)
	reqId := param["id"]
	var reqVals map[string]string
	response := make(map[string]string)

	if req.Method == "PUT" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&reqVals)
		if err != nil {
			log.Fatalf("Error couldnt decode: %s", err)
			return
		}
		reqIp := reqVals["socket-address"]

		// if the request is invalid
		if _, ok := shardSplit[reqId]; !ok || containsVal(reqIp, replicaArray) < 0 {
			w.WriteHeader(http.StatusNotFound)
		} else {
			//add node to shard and update all other replicas in view
			w.WriteHeader(http.StatusOK)
			shardSplit[reqId] = append(shardSplit[reqId], reqIp)

			// creating broadcast variables needed
			broadcastBody := make(map[string]string)
			broadcastBody["add-ip"] = reqIp
			bBody, err := json.Marshal(broadcastBody)
			if err != nil {
				log.Fatalf("Error here: %s", err)
			}

			// broadcast the new shard replica IP array to all replicas
			for _, replicaIp := range replicaArray {
				if replicaIp != sAddress {
					fmt.Println("forwarding to ", replicaIp)
					forwardReq("PUT", replicaIp, fmt.Sprintf("/shard/broadcast/add/%s", reqId), bBody)
				}
			}

			// response
			response["result"] = "node added to shard"
			jsonResponse, err := json.Marshal(response)
			if err != nil {
				log.Fatalf("Error here: %s", err)
			}
			w.Write(jsonResponse)
		}
	}
}

// Handler function that will handle reshard of our system
func handleReshard(w http.ResponseWriter, req *http.Request) {
	// create dict variable to hold inputted value
	var reqVals map[string]int
	response := make(map[string]string)

	if req.Method == "PUT" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&reqVals)
		if err != nil {
			log.Fatalf("Error couldnt decode: %s", err)
			return
		}

		newShardCount := reqVals["shard-count"]

		// first checking if fault tolerance invariant is violated
		// i.e. too many shards and too few replicas
		if float32(len(replicaArray))/float32(newShardCount) < 2.0 {
			response["error"] = "Not enough nodes to provide fault tolerance with requested shard count"
			w.WriteHeader(http.StatusBadRequest)
		} else {
			// creating new store variable to hold the result of combining all the stores of all shards
			entireStore := make(map[string]interface{})

			// loop thru all shards, and their respective replica IP arrays
			for _, element := range shardSplit {
				// finding a shard that is not the one we are on
				// in this case, we must send a request to one of the replicas in that shard to give us their kvs
				// getting the kvs of the first replicaIP in the shard's replica IP array
				tempStore := getReplicaKVS(element[0])

				// combining the entireStore with the store we got from the replica
				for k, v := range tempStore {
					entireStore[k] = v
				}
			}

			// splitting the nodes to the new shard count, reassigning our current shard
			splitNodes(newShardCount)
			currentShard = ipToShardMap[sAddress]

			// creating variables neeed to hash the entire store
			var shardStore = make(map[string]interface{})
			storeMap := make(map[string]map[string]interface{})

			// making an array of stores, that we will later send out
			for _, shard := range hashIndexArr {
				storeMap[shard] = make(map[string]interface{})
			}

			// going thru and hashing each kv pair in the store, and adding the values
			// to their respoective stores based on index in the store array
			for key, val := range entireStore {
				hashedKey := hash(key)
				shardKeyBelongsTo := hashIndexArr[hashedKey]
				shardStore = storeMap[shardKeyBelongsTo]
				shardStore[key] = val
			}

			// deebug
			fmt.Println("storemap === ", storeMap)
			fmt.Println("shard split == ", shardSplit)

			// creating new requests to send to replica's, i.e. their stores
			reshardBody := make(map[string]interface{})
			reshardBody["shard-count"] = newShardCount

			// looping thru all shards
			for shardName, shard := range shardSplit {
				// assigning their new shard stores
				reshardBody["shard-store"] = storeMap[shardName]
				jsonReshardBody, err := json.Marshal(reshardBody)
				if err != nil {
					log.Fatalf("Error here: %s", err)
				}

				// broadcasting to each IP in that shard
				for _, replicaIP := range shard {
					forwardReq("PUT", replicaIP, "/resplitNodes", jsonReshardBody)
				}
			}
		}

		// writing json response
		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Fatalf("Error here: %s", err)
		}
		w.Write(jsonResponse)
	}
}
