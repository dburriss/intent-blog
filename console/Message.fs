module Message

open Microsoft.Azure
open Microsoft.Azure.Storage
open Microsoft.Azure.Storage.Queue
open Domain
open Newtonsoft.Json

let init conn = 
    let storageAccount = CloudStorageAccount.Parse(conn)
    let queueClient = storageAccount.CreateCloudQueueClient()
    queueClient

let personCreated (queue:CloudQueue) (person:Person) = 
    let msg = person |> JsonConvert.SerializeObject
    let message = CloudQueueMessage(msg)
    
    try
        queue.AddMessage(message)
        Ok person
    with
    | ex -> Error ex    
    