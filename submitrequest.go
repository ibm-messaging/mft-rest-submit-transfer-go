/*
© Copyright IBM Corporation 2022, 2022
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
* This file contains the source code for submitting a transfer request
* to a IBM MQ Managed File Transfer agent using REST APIs and then query
* the status of transfer.
*
* The program does the following:
* 1) Builds a JSON object containing the transfer request.
* 2) Builds a HTTP POST request and then submits it to IBM MQ Web Server.
* 3) Builds a HTTP GET request with URL returned in step #2 above to query
*    the status of a transfer. If transfer status is not yet available, the
*    program waits for 5 seconds and resubmits the HTTP GET request again to
*    query the transfer status.
*
* This program assumes the following:
* 1) MFT network has been setup with at least two agents.
* 2) Basic authentication for REST APIs has been configured.
* 3) MQ Web Server has been configured and started.
 */
package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	// Third party libraries used
	j "github.com/ricardolonga/jsongo"
	"github.com/tidwall/gjson"
)

/**
* Constants used by this application. Modify per your requirement.
 */
const mqRestXferUrl = "http://localhost:8080/ibmmq/rest/v2/admin/mft/transfer"
const mqWebUserId = "mqmftadminusr"
const mqWebPassword = "mqmftpassw0rd"
const sourceAgentName = "SRC"
const destinationAgentName = "DEST"
const sourceQMName = "SRCQM"
const destinationQMName = "DESTQM"
const sourceItemName = "/usr/srcdir"
const destinationItemName = "/usr/destdir"
const sourceItemType = "file"
const destinationItemType = "directory"

/**
* Main entry point
 */
func main() {
	// Build a transfer request and put to agent's command queue
	transferRequest := buildTransferJsonRequest()
	// Post transfer request. Rerturn value will have URL to retrieve transfer status.
	retCode, transferUrl := postTransferRequest(transferRequest)
	if retCode == http.StatusAccepted {
		// Requested submitted successfully. Now look for status of transfer
		respCode := waitForTransferStatus(transferUrl)
		// If response was anything other 200, then rerun the request as the transfer
		// may not have started yet.
		if respCode != http.StatusOK {
			// Wiat for 5 seconds and resubmit the HTTP GET request again
			time.Sleep(5 * time.Second)
			// Resubmit the transfer status GET request
			respCode = waitForTransferStatus(transferUrl)
		}
	}
}

// Build a simmple transfer JSON request.
func buildTransferJsonRequest() string {
	// Source agent attributes
	sourceAgent := j.Object().Put("qmgrName", sourceQMName)
	sourceAgent.Put("name", sourceAgentName)
	xferRequest := j.Object().Put("sourceAgent", sourceAgent)

	// Destination agent attributes
	destAgent := j.Object().Put("qmgrName", destinationQMName)
	destAgent.Put("name", destinationAgentName)
	xferRequest = xferRequest.Put("destinationAgent", destAgent)

	// Source item attributes
	sourceItem := j.Object().Put("name", sourceItemName)
	sourceItem.Put("type", sourceItemType)

	// Destination item attributes
	destItem := j.Object().Put("name", destinationItemName)
	destItem.Put("type", destinationItemType)

	// Set source and destination to item group
	item := j.Object().Put("source", sourceItem)
	item.Put("destination", destItem)

	// Set item in to the transfer item array
	itemsArray := j.Array().Put(item)
	xfertSetItems := j.Object().Put("item", itemsArray)

	// Set transfer items array to transfer set
	xferRequest.Put("transferSet", xfertSetItems)
	//Return JSON object as string
	return xferRequest.String()
}

/* Build a HTTP request with given inputs
* httpVerb - Value can be GET or POST
* url      - Url to which request will be submitted
* body     - Body of the request to be sent
* userId   - UserId for basic authentication
* password - Password for basic authentication
 */
func buildHTTPRequestHeader(httpVerb string, url string, body string, userId string, password string) (*http.Request, error) {
	var requestBody *bytes.Buffer = nil
	if len(body) > 0 {
		requestBody = bytes.NewBufferString(body)
	} else {
		requestBody = bytes.NewBufferString("")
	}

	httpRequest, errReq := http.NewRequest(httpVerb, url, requestBody)
	if errReq == nil {
		// Set the required HTTP headers
		uidPwd := userId + ":" + password
		uidPwdArr := []byte(uidPwd)
		encodedCreds := make([]byte, base64.StdEncoding.EncodedLen(len(uidPwd)))
		base64.StdEncoding.Encode(encodedCreds, uidPwdArr)
		httpRequest.Header.Set("Authorization", "Basic "+string(encodedCreds))
		// csrf-token must be set but can be blank
		httpRequest.Header.Set("ibm-mq-rest-csrf-token", "")
		httpRequest.Header.Set("Content-Type", "application/json")
	}
	return httpRequest, errReq
}

/* Submit transfer request.
*  xferRequestJson - Transfer request in JSON format.
 */
func postTransferRequest(xferRequestJson string) (int, string) {
	xferReqURL := mqRestXferUrl
	httpPOST, errPOST := buildHTTPRequestHeader("POST", xferReqURL, xferRequestJson, mqWebUserId, mqWebPassword)
	if errPOST != nil {
		fmt.Printf("Error occured creating HTTP request. The error is %v\n", errPOST)
		return -1, ""
	}
	postClient := &http.Client{}
	respPost, errPost := postClient.Do(httpPOST)
	if errPost != nil {
		fmt.Printf("An error occured while publishing transfer logs to %s. The error is: %v\n", xferReqURL, errPost)
		return -1, ""
	}
	defer respPost.Body.Close()

	fmt.Printf("Submitted transfer request to: %v\n", xferReqURL)
	//Read the response body
	var transferStatusUrl string = ""
	var retCode int = -1
	_, err := ioutil.ReadAll(respPost.Body)
	if err != nil {
		fmt.Printf("An error occurred while reading response from server %s. The error is: %v\n", xferReqURL, err)
	} else {
		fmt.Printf("HTTP response received. Status: %v\n", respPost.Status)
		retCode = respPost.StatusCode
		if respPost.StatusCode == http.StatusAccepted {
			transferStatusUrl = respPost.Header.Get("location")
			fmt.Printf("Transfer URL:%v\n", transferStatusUrl)
		}
	}
	return retCode, transferStatusUrl
}

/**
* Issue HTTP GET request to retrieve the status of transfer.
* transferUrl - URL to query transfer status. This URL is returned by POST verb request.
 */
func waitForTransferStatus(transferUrl string) int {
	// Build HTTP GET request to query all attributes of transfer.
	fmt.Printf("Querying status of transfer\n")
	httpGET, errGET := buildHTTPRequestHeader("GET", transferUrl+"?attributes=*", "", mqWebUserId, mqWebPassword)
	if errGET != nil {
		fmt.Printf("Error occured creating HTTP request. The error is %v\n", errGET)
		return -1
	}
	getClient := &http.Client{}
	// Run the request and handle errors.
	respGET, errGET := getClient.Do(httpGET)
	if errGET != nil {
		fmt.Printf("An error occured while publishing transfer logs to %s. The error is: %v\n", transferUrl, errGET)
		return -1
	}
	defer respGET.Body.Close()

	// Verify the status code
	if respGET.StatusCode == http.StatusOK {
		respBody, errGET := ioutil.ReadAll(respGET.Body)
		if errGET == nil {
			respJson := gjson.Get(string(respBody), "transfer").Array()
			status := gjson.Get(respJson[0].String(), "status.state")
			id := gjson.Get(respJson[0].String(), "id")
			fmt.Printf("Status of transfer with ID %v is %v\n", id.String(), status.String())
			if !strings.EqualFold(status.String(), "successful") {
				// Display additional details if the status is not successful
				statusDescription := gjson.Get(respJson[0].String(), "status.description")
				fmt.Printf("%s\nFollowing errors occurred:\n", statusDescription.String())
				transferItems := gjson.Get(respJson[0].String(), "transferSet.item").Array()
				if len(transferItems) > 0 {
					itemCount := len(transferItems)
					for index := 0; index < itemCount; index++ {
						if !strings.EqualFold(transferItems[index].Get("status.state").String(), "successful") {
							fmt.Printf("%s\n", transferItems[index].Get("status.description").String())
						}
					}
				}
			}
		} else {
			fmt.Printf("An error occurred while reading response from server %s. The error is: %v\n", transferUrl, errGET)
		}
	} else {
		fmt.Printf("Response code received: %v\n", respGET.Status)
	}
	return respGET.StatusCode
}
