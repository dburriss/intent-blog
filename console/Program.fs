// Learn more about F# at http://fsharp.org

open System
open Domain
open System.Data.Common

let tap f x =
        f x
        x

let makeId() = Guid.NewGuid() |> string

let ``execution persists,does not notify`` dbConnection queue =
    // save to database
    // it could then fail
    // put on queue
    let id = makeId()
    {
        id = id
        name = "Bob Doe"
        email = sprintf "%s@doe.com" id
    }
    |> Data.createPerson dbConnection None
    |> tap (fun _ -> failwith "Failed before sending message")
    |> Result.bind (Message.personCreated queue)
    |> ignore
    
let ``execution notifies, does not persist`` dbConnection queue =
    // put on queue
    // it could then fail
    // save to database
    let id = makeId()
    {
        id = id
        name = "Jane Doe"
        email = sprintf "%s@doe.com" id
    }
    |> Message.personCreated queue
    |> tap (fun _ -> failwith "Failed before sending message")
    |> Result.bind (Data.createPerson dbConnection None)
    |> ignore

let handleIntent connection queue (id,intent) =
    match intent with
    | Pending person -> 
        Message.personCreated queue person |> ignore
        Data.markCreatePersonIntentDone connection id (Complete person) |> ignore
        printfn "%A intent sent" person
    | Complete _ -> failwith "These should not be queried"
        

let processIntents (dbConnection:DbConnection) queue =
    let intentsR = Data.getCreatePersonIntents dbConnection
    match intentsR with
    | Error ex -> raise ex
    | Ok intents -> intents |> Seq.iter (handleIntent dbConnection queue)

let ``execution persists person and intent to send message`` (dbConnection:DbConnection) queue =
    // save to database with intent
    // intent puts on queue
    let id = makeId()
    let person = 
        {
            id = id
            name = "K Doe"
            email = sprintf "%s@doe.com" id
        }
    use transaction = dbConnection.BeginTransaction()
    let txn = Some transaction
    person    
    |> Data.createPerson dbConnection txn
    |> Result.map (fun p -> Data.createPersonIntent dbConnection txn (Pending p))
    |> ignore
    transaction.Commit()
    
let makeDb() =
    let conn = Data.conn "db"
    conn.Open()
     
    Data.createPeopleTable conn |> (function | Ok x -> ignore | Error ex -> raise ex) |> ignore
    Data.createIntentTable conn |> (function | Ok x -> ignore | Error ex -> raise ex) |> ignore
    conn

let makeQueue() =
    let queueClient = Message.init "UseDevelopmentStorage=true"
    let queue = queueClient.GetQueueReference("personcreated")
    queue

[<EntryPoint>]
let main argv =

    let conn = makeDb()
    let queue = makeQueue()

    //``execution persists,does not notify`` conn queue
    //``execution notifies, does not persist`` conn queue
    ``execution persists person and intent to send message`` conn queue
    processIntents conn queue
    conn.Close()

    0 // return an integer exit code
