module Data

open System
open Domain
open Microsoft.Data.Sqlite
open Dapper
open System.Data.Common
open Newtonsoft.Json
open System.Collections.Generic

let conn file = 
    let cs = sprintf "Data Source=./%s.sqlite;" file
    new SqliteConnection(cs)

let execute (connection:#DbConnection) (sql:string) (parameters:_) (transaction:#DbTransaction option) =
    try
        let result = 
            match transaction with
            | None -> connection.Execute(sql, parameters)
            | Some txn -> connection.Execute(sql, parameters, txn)
            
        Ok result
    with
    | ex -> raise ex

let query (connection:#DbConnection) (sql:string) (parameters: IDictionary<string, obj> option) : Result<seq<'T>,exn> =
    try
        let result =
            match parameters with
            | Some p -> connection.Query<'T>(sql, p)
            | None -> connection.Query<'T>(sql)
        Ok result
    with
    | ex -> raise ex

let createPeopleTable (connection:#DbConnection) =
    let sql = "CREATE TABLE IF NOT EXISTS people (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 email TEXT NOT NULL UNIQUE
                );"
    execute connection sql None None

let createIntentTable (connection:#DbConnection) =
    let sql = "CREATE TABLE IF NOT EXISTS intents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 iscomplete INTEGER NOT NULL DEFAULT 0,
                 intenttype TEXT NOT NULL,
                 intent BLOB NOT NULL
                );"
    execute connection sql None None

let createPerson (connection:#DbConnection) (transaction:#DbTransaction option) person =
    let data = [("@id",box person.id);("@name",box person.name);("@email",box person.email)] |> dict |> fun d -> DynamicParameters(d)
    let sql = "INSERT INTO people (id,name,email) VALUES (@id,@name,@email);"
    execute connection sql data transaction
    |> Result.map (fun _ -> person)

let createIntent (connection:#DbConnection) (transaction:#DbTransaction option) (intent:string) (type':string)=
    let data = [("@intent",box intent);("@intenttype",box type')] |> dict |> fun d -> DynamicParameters(d)
    let sql = "INSERT INTO intents (intent,intenttype) VALUES (@intent,@intenttype);"
    execute connection sql data transaction

let updateIntent (connection:#DbConnection) (id:string) (intent:string) (isComplete:bool) =
    let data = [("@id", box id);("@iscomplete",box (isComplete |> Convert.ToInt32));("@intent",box intent)] |> dict |> fun d -> DynamicParameters(d)
    let sql = "UPDATE intents SET intent = @intent, iscomplete = @iscomplete WHERE id = @id;"
    execute connection sql data None

let createPersonIntent (connection:#DbConnection) (transaction:#DbTransaction option) (intent:IntentOfPersonCreated) =
    let intent' = intent |> JsonConvert.SerializeObject
    createIntent connection transaction intent' "create-person"

[<CLIMutable>]
type IntentRow = {
    id:string
    iscomplete:bool
    intenttype:string
    intent:string
}

let private mapIntentRow (row:IntentRow) =
    let intent = row.intent |> JsonConvert.DeserializeObject<IntentOfPersonCreated>
    (row.id, intent)

let private mapIntentRows rows = rows |> Seq.map mapIntentRow

let getCreatePersonIntents (connection:#DbConnection) : Result<seq<(string*IntentOfPersonCreated)>,exn> = 
    let sql = "SELECT id,iscomplete,intenttype,intent FROM intents WHERE iscomplete = 0 AND intenttype = 'create-person'"
    let q : Result<seq<IntentRow>,exn> = query connection sql None
    q |> Result.map mapIntentRows

let markCreatePersonIntentDone (connection:#DbConnection) (id:string) (intent:IntentOfPersonCreated) =
    let intent' = intent |> JsonConvert.SerializeObject
    updateIntent connection id intent' true

    