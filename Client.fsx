#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"
#r "nuget: MathNet.Numerics"

open System
open Akka.FSharp
open Akka.Actor
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets
open MathNet.Numerics.Distributions
open MathNet.Numerics.Random


// Global Variables
let ServerAddress = ( fsi.CommandLineArgs.[2] + ":9903" )
let mutable Users = [|"None"|]
let Topics = [|"None"; "Ufl"; "CISE"; "TheHub"; "Dosp"; "MSinCS"; "GatorNation"|]
let mutable maxTweets = 0


let mutable countSimulation = 1

// State of Client
type Client = {
    Name: string
    Id: int
    LastTweet: (int * bool)
    LoggedIn: bool
}


// Messages
type Message =
    | DecideNextOperation
    // Messages for interaction with self
    | Login
    | Signout
    | Publish
    | Request
    | Share
    | Filter
    | Subscribe
    // Messages for interaction with server
    | LoginOrSignup of IActorRef * string
    | LoginDone of int
    | Logout of int
    | LogoutDone
    | Tweet of int * string * string
    | TweetDone of int
    | Timeline of int
    | TimelineDone of List<int * string * string>
    | Retweet of int * int
    | RetweetDone of int * string * string
    | Query of int * string
    | QueryDone of List<int * string * string>
    | Follow of IActorRef * int * string
    | FollowDone of bool * string
    | Update of int * string * string
    | EndSimulation of int


// List of operations
let operations = [|Login; Signout; Publish; Request; Share; Filter|]
let mutable numOfSubscribers = Map.empty


// Calculates IP of the localhost
let localIPs () =
    let networkInterfaces = NetworkInterface.GetAllNetworkInterfaces()
                            |> Array.filter(fun iface -> iface.OperationalStatus.Equals(OperationalStatus.Up))

    let addresses = seq {
        for iface in networkInterfaces do
            for unicastAddr in iface.GetIPProperties().UnicastAddresses do
                yield unicastAddr.Address}

    addresses
    |> Seq.filter(fun addr -> addr.AddressFamily.Equals(AddressFamily.InterNetwork))
    |> Seq.filter(IPAddress.IsLoopback >> not)
    |> Seq.head


// Configuration
let configurationString (port:string)= 
    let str1 = @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
                serializers {
                    hyperion = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
                }
                serialization-bindings {
                    ""System.Object"" = hyperion
                }
            }
            remote {
                helios.tcp {
                    port = "

    let str2 = @"
                    hostname ="
    let str3 = @"
            }
        }
    }"
    Configuration.parse (str1+port+str2+localIPs().ToString()+str3)


// Function definitions
let selectRandom(arr: 'T []) =
    let list = List.ofArray(arr)
    let seq = List.toSeq(list)
    let rnd = Random()
    seq |> Seq.sortBy (fun _ -> rnd.Next()) |> Seq.head


// Client Definition
let client (serverIpAddress: string, systemRef: ActorSystem, cname: string) (mailbox:Actor<_>) =
    let serverRef = systemRef.ActorSelection($"akka.tcp://Server@{serverIpAddress}/user/server")
    
    let rec loop state = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | DecideNextOperation ->
            if (maxTweets >= 1000) then
                printfn "Maximum tweets at Userid %d reached in the system. Ending simulation, count= %d." state.Id countSimulation
                countSimulation <- countSimulation + 1
                serverRef <! EndSimulation (state.Id)
                mailbox.Self <! Signout
                mailbox.Self <! PoisonPill.Instance
                //systemRef.Terminate()
                return! loop state

            let mutable ops = operations
            let numSubs = snd ( numOfSubscribers.TryGetValue(state.Name) )
            //printfn "[%s] %d" state.Name numSubs
            if (numSubs > 1) then
                for i in 1..numSubs do
                    if (i%3 <> 0) then
                        Array.append ops [|Publish|]
                    else
                        Array.append ops [|Share|]
            let message = selectRandom(ops)
            System.Threading.Thread.Sleep 500
            mailbox.Self <! message
            return! loop state
        
        | Login ->
            if (state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            serverRef <! LoginOrSignup (mailbox.Self, state.Name)
            return! loop state
        
        | LoginDone (id: int) ->
            printfn "[%s] %s LOGGED IN" state.Name state.Name
            mailbox.Self <! DecideNextOperation
            return! loop {state with LoggedIn=true; Id=id}
        
        | Publish ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            let mutable randomTweet = ("Random tweet from " + state.Name)
            let randomTopic = selectRandom(Topics)
            if (randomTopic <> "None") then
                randomTweet <- randomTweet + (" on #" + randomTopic)
            let randomMention = selectRandom(Users)
            if (randomMention <> "None") then
                randomTweet <- randomTweet + (" mentioning @" + randomMention)
            serverRef <! Tweet (state.Id, state.Name, randomTweet)
            return! loop state
        
        | TweetDone (tweetId: int) ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            printfn "[%s] %s PUBLISHED Tweet #%d" state.Name state.Name tweetId
            maxTweets <- maxTweets + 1
            mailbox.Self <! DecideNextOperation
            return! loop state

        | Request ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            serverRef <! Timeline state.Id
            return! loop state

        | TimelineDone (tweets: List<int * string * string>) ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            let mutable lastTw = -1
            printfn "[%s] ------------- %s 's Timeline -------------" state.Name state.Name
            for tweet in tweets do
                match tweet with
                | (twID, twAuth, twCont) ->
                    printfn "[%s] %s tweeted \"%s\"" state.Name twAuth twCont
                    lastTw <- twID
            printfn "[%s] ------------------------------------------" state.Name
            mailbox.Self <! DecideNextOperation
            return! loop {state with LastTweet=(lastTw, false)}

        | Share ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            let rtwStat = snd state.LastTweet
            let rtwId = fst state.LastTweet
            if (rtwStat || (rtwId = -1)) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            else
                //printfn "[%s] %s is sending RETWEET request for TWEET #%d to the server....." state.Name rtwId
                serverRef <! Retweet (state.Id, rtwId)
                return! loop {state with LastTweet=(rtwId, true)}

        | RetweetDone (rtwId: int, rtwAuth: string, rtwCont: string) ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            if (rtwId <> -1) then
                maxTweets <- maxTweets + 1
                printfn "[%s] %s RE-TWEETED \"%s\" authored by %s" state.Name state.Name rtwCont rtwAuth
            mailbox.Self <! DecideNextOperation
            return! loop state

        | Filter ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            let mutable option = selectRandom([|1;2;3|])
            let mutable filter = ""
            match option with
            | 1 ->
                let mutable rndUser = "None"
                while (rndUser = "None") do
                    rndUser <- selectRandom(Users)
                filter <- rndUser
                printfn "[%s] %s is QUERYING all tweets published by %s ....." state.Name state.Name filter
            | 2 ->
                let mutable rndTopic = "None"
                while (rndTopic = "None") do
                    rndTopic <- selectRandom(Topics)
                filter <- "#" + rndTopic
                printfn "[%s] %s is QUERYING all tweets on %s ....." state.Name state.Name filter
            | 3 ->
                filter <- "@" + state.Name
                option <- 2
                printfn "[%s] %s QUERYING all tweets that mention %s ....." state.Name state.Name filter
            serverRef <! Query (option, filter)
            return! loop state

        | QueryDone (tweets: List<int * string * string>) ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            printfn "[%s] ------------- %s 's Search Results -------------" state.Name state.Name
            for tweet in tweets do
                match tweet with
                | (twId, twAuth, twCont) ->
                    printfn "[%s] %s tweeted \"%s\"" state.Name twAuth twCont
            printfn "[%s] ------------------------------------------------" state.Name
            mailbox.Self <! DecideNextOperation
            return! loop state

        | Subscribe ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            let mutable randomUser = "None"
            while ((randomUser = "None") || (randomUser = state.Name)) do
                randomUser <- selectRandom(Users)
            serverRef <! Follow (mailbox.Self, state.Id, randomUser)
            return! loop state

        | FollowDone (status: bool, user: string) ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            if (status) then
                printfn "[%s] %s is now FOLLOWING %s" state.Name state.Name user
            mailbox.Self <! DecideNextOperation
            return! loop state

        | Signout ->
            if (not state.LoggedIn) then
                mailbox.Self <! DecideNextOperation
                return! loop state
            
            serverRef <! Logout state.Id
            return! loop state

        | LogoutDone ->
            printfn "[%s] %s LOGGED OUT" state.Name state.Name
            mailbox.Self <! DecideNextOperation
            return! loop {state with LoggedIn=false}

        | Update (twId: int, twAuth: string, twCont: string) ->
            printfn "[%s] %s JUST TWEETED \"%s\"" state.Name twAuth twCont
            mailbox.Self <! DecideNextOperation
            return! loop {state with LastTweet=(twId, false)}
        
        | _ ->  
            failwith "[LOG] Unknown message."
    }

    loop {Name=cname; Id= -1; LastTweet=(-1, false); LoggedIn=false}


let start (args: string[]) =
    let serverIpWithPort = ServerAddress
    let systemRef = System.create "Client" (configurationString "5556")
    let mutable clientRefs: IActorRef [] = [||]
    let serverRef = systemRef.ActorSelection($"akka.tcp://Server@{serverIpWithPort}/user/server")

    let numUsers = int(args.[1])
    for i in 1..numUsers do
        let clientRef = spawn systemRef ("Client"+string(i)) (client(serverIpWithPort, systemRef, "User_"+string(i)))
        Users <- Array.append Users [|"User_"+string(i)|]
        clientRefs <- Array.append clientRefs [|clientRef|]
        serverRef <! LoginOrSignup (clientRef, ("User_"+string(i)))

    let zipf = new Zipf(1.0, numUsers)
    //printfn "%A" zipf
    for i in 1..numUsers do
        let numSubs = (zipf.Probability(i)* (float (numUsers))) |> int
        numOfSubscribers <- numOfSubscribers.Add( ("User_"+string(i)), numSubs )
        //printfn "Number of subscribers for User_%d will be %d" i numSubs
        let mutable subSet = Set.empty
        while (subSet.Count <> numSubs) do
            let rndUser = selectRandom([|1..numUsers|])
            if not (subSet.Contains(rndUser)) then
                serverRef <! Follow (clientRefs.[i-1], rndUser, Users.[i])
                subSet <- subSet.Add(rndUser)
            //printfn "Follower set for User_%d is %A" i subSet
        //printfn "----------------------------------------"
    
    for i in 1..numUsers do
        let cl = clientRefs.[i-1]
        cl <! DecideNextOperation

    //printfn "List of users in the system : %A" Users
    
    systemRef.WhenTerminated.Wait()

start(fsi.CommandLineArgs)
