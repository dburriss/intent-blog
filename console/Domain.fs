module Domain

//[<CLIMutable>]
type Person = {
    id:string
    name:string
    email:string
}

type IntentOfPersonCreated = 
| Pending of Person
| Complete of Person

