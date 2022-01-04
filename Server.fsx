#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"

open Akka.FSharp
open Akka.Actor
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets
open System
open System.Data
open System.IO

type Performance = {
    UserId: int
    FollowersOfUser: int
    LiveUserCount: int
    TimeTaken: Double
    IsRetweet: bool
}

// Enumerations
// State of Server actor
type ServerState = {
    Users: Map<int, IActorRef>
    PerformanceRecording: List<Performance>
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

type Tweet = {
    UserId: int
    Tweet: string
}

type User = {
    UserId : int
    Name : string
}

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

// Node configuration
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

// Port Numbers for Server, client
let serverPort = "9903"
let clientPort = "5556"

let stop(systemRef:ActorSystem) =
    systemRef.Terminate()
    |> ignore

let mutable mapRef: Map<int, IActorRef> = Map.empty

// users table
let usersTable = new DataTable()
let userIdCol = usersTable.Columns.Add("UserId", typeof<int>);
let nameCol = usersTable.Columns.Add("Name", typeof<string>);
usersTable.PrimaryKey <- [|userIdCol|]
userIdCol.AutoIncrement = true
userIdCol.AutoIncrementSeed = int64(1)
userIdCol.AutoIncrementStep = int64(1)
nameCol.AllowDBNull = false
nameCol.Unique = true

// tweets table 
let tweetsTable = new DataTable()
let tweetIdCol = tweetsTable.Columns.Add("TweetId", typeof<int>);
tweetsTable.Columns.Add("Content", typeof<string>);
tweetsTable.Columns.Add("RetweetId", typeof<int>);
let tweetUserIdCol = tweetsTable.Columns.Add("UserId", typeof<int>);
tweetsTable.PrimaryKey <- [|tweetIdCol|]
tweetIdCol.AutoIncrement = true
tweetIdCol.AutoIncrementSeed = int64(1)
tweetIdCol.AutoIncrementStep = int64(1)
tweetIdCol.AllowDBNull = false
tweetUserIdCol.AllowDBNull = false

// social table 
let socialTable = new DataTable()
let socialIdCol = socialTable.Columns.Add("SocialId", typeof<int>);
let followingUserIdCol = socialTable.Columns.Add("FollowingUserId", typeof<int>);
let followerUserIdCol = socialTable.Columns.Add("FollowerUserId", typeof<int>);
socialTable.PrimaryKey <- [|socialIdCol|]
socialIdCol.AutoIncrement = true
socialIdCol.AutoIncrementSeed = int64(1)
socialIdCol.AutoIncrementStep = int64(1)
socialIdCol.AllowDBNull = false
followingUserIdCol.AllowDBNull = false
followerUserIdCol.AllowDBNull = false

let loginOrSignupFn (userName: string) = 
    let exprToFindUser = "Name = '"+userName+"'"
    let foundUserRow = (usersTable.Select(exprToFindUser))
    let userRowSeq = seq { yield! foundUserRow}
    let userRowSeqLength = userRowSeq |> Seq.length
    if userRowSeqLength > 0 then
        let row = userRowSeq |> Seq.head 
        row.Field(usersTable.Columns.Item(0)) |> int
    else 
        let newUserRow = usersTable.NewRow()
        newUserRow.SetField("UserId",usersTable.Rows.Count+1)
        newUserRow.SetField("Name",userName)
        usersTable.Rows.Add(newUserRow)
        usersTable.Rows.Count

let tweetFn (userId: int, userName: string, str: string) = 
    let newTweetRow = tweetsTable.NewRow()
    let tweetId = tweetsTable.Rows.Count+1
    newTweetRow.SetField("TweetId",tweetId)
    newTweetRow.SetField("UserId",userId)
    newTweetRow.SetField("Content",str)
    tweetsTable.Rows.Add(newTweetRow)
    (tweetId, userName, str)

let followFn (followerUserId: int, followingUserName: string) = 
    let exprToFindUser = "Name = '"+followingUserName+"'"
    let foundUser = (usersTable.Select(exprToFindUser))
    let userRowSeq = seq { yield! foundUser}
    if (userRowSeq |> Seq.length) > 0 then 
        let userRowSeqHead = userRowSeq |> Seq.head
        let followingUserId = userRowSeqHead.Field(usersTable.Columns.Item(0)).ToString()
        let exprToFindSocial = "FollowerUserId = '"+followerUserId.ToString()+"' AND FollowingUserId = '"+followingUserId+"'"
        let foundSocial = (socialTable.Select(exprToFindSocial))
        let socialRowSeq = seq { yield! foundSocial}
        let socialRowSeqLength = socialRowSeq |> Seq.length
        if socialRowSeqLength = 0 then
            let newSocialRow = socialTable.NewRow()
            newSocialRow.SetField("SocialId",socialTable.Rows.Count+1)
            newSocialRow.SetField("FollowerUserId",followerUserId)
            newSocialRow.SetField("FollowingUserId",followingUserId)
            socialTable.Rows.Add(newSocialRow)
            true
        else
            false
    else
        false

let retweetFn (userId: int, tweetId: int) = 
    let mutable exprToFindTweet = "TweetId = '"+tweetId.ToString()+"' AND UserId ='"+userId.ToString()+"'"
    let mutable foundTweet = (tweetsTable.Select(exprToFindTweet))
    let mutable tweetRowSeq = seq { yield! foundTweet}
    let tweetRowSeqLength = tweetRowSeq |> Seq.length
    if tweetRowSeqLength = 0 then
        exprToFindTweet <- "TweetId = '"+tweetId.ToString()+"'"
        foundTweet <- (tweetsTable.Select(exprToFindTweet))
        tweetRowSeq <- seq { yield! foundTweet}
        let tweet = tweetRowSeq |> Seq.head 
        let content = tweet.Field(tweetsTable.Columns.Item(1)).ToString()
        let user = int(tweet.Field(tweetsTable.Columns.Item(3)))
        let newTweetRow = tweetsTable.NewRow()
        newTweetRow.SetField("TweetId",tweetsTable.Rows.Count+1)
        newTweetRow.SetField("UserId",userId)
        newTweetRow.SetField("RetweetId",tweetId)
        tweetsTable.Rows.Add(newTweetRow)
        let exprToFindUser = "UserId = '"+user.ToString()+"'"
        let foundUser = (usersTable.Select(exprToFindUser))
        let userRowSeq = seq { yield! foundUser}
        let userRowSeqHead = userRowSeq |> Seq.head
        (tweetId, userRowSeqHead.Field(usersTable.Columns.Item(1)).ToString(), content)
    else 
        (-1, "", "")

let getFollowingFn (followerUserId: int) = 
    let exprToFindSocial = "FollowerUserId = '"+followerUserId.ToString()+"'"
    let foundSocial = (socialTable.Select(exprToFindSocial))
    let socialRowSeq = seq { yield! foundSocial}
    let mutable followingUserIdList = []
    for x in socialRowSeq do
        followingUserIdList  <- [int(x.Field(socialTable.Columns.Item(1)))] |> List.append followingUserIdList
    followingUserIdList

let getFollowersFn (followingUserId: int) = 
    let exprToFindSocial = "FollowingUserId = '"+followingUserId.ToString()+"'"
    let foundSocial = (socialTable.Select(exprToFindSocial))
    let socialRowSeq = seq { yield! foundSocial}
    let mutable followerUserIdList = []
    for x in socialRowSeq do
        followerUserIdList  <- [int(x.Field(socialTable.Columns.Item(2)))] |> List.append followerUserIdList
    followerUserIdList

let getUsernameFn (userId: int) =
    let exprToFindUser = "UserId = '"+userId.ToString()+"'"
    let foundUser = (usersTable.Select(exprToFindUser))
    let userRowSeq = seq { yield! foundUser} |> Seq.head
    userRowSeq.Field(usersTable.Columns.Item(1)).ToString()

let getUserIdFn (userName: string) =
    let exprToFindUser = "Name = '"+userName.ToString()+"'"
    let foundUser = (usersTable.Select(exprToFindUser))
    let userRowSeq = seq { yield! foundUser} |> Seq.head
    int(userRowSeq.Field(usersTable.Columns.Item(0)))

let getTweetByUserFn (userId: int) = 
    let mutable exprToFindTweet = "UserId = '"+userId.ToString()+"' AND Content IS NOT NULL"
    let mutable foundTweet = (tweetsTable.Select(exprToFindTweet))
    let mutable tweetRowSeq = seq { yield! foundTweet}
    let mutable userName = getUsernameFn(userId)
    let mutable tweetsList = []
    for x in tweetRowSeq do
        tweetsList  <- [int(x.Field(tweetsTable.Columns.Item(0))), userName, x.Field(tweetsTable.Columns.Item(1)).ToString()] |> List.append tweetsList
    exprToFindTweet <- "UserId = '"+userId.ToString()+"' AND RetweetId IS NOT NULL"
    foundTweet <- (tweetsTable.Select(exprToFindTweet))
    tweetRowSeq <- seq { yield! foundTweet}
    for x in tweetRowSeq do
        exprToFindTweet <- "TweetId = '"+x.Field(tweetsTable.Columns.Item(2)).ToString()+"'"
        let found = (tweetsTable.Select(exprToFindTweet))
        let foundSeq =  seq { yield! found} |> Seq.head
        let userName2 = getUsernameFn(int(foundSeq.Field(tweetsTable.Columns.Item(3))))
        tweetsList  <- [int(foundSeq.Field(tweetsTable.Columns.Item(0))), userName2, foundSeq.Field(tweetsTable.Columns.Item(1)).ToString()] |> List.append tweetsList
    tweetsList

let getTweetByPatternFn (str: string) = 
    let exprToFindTweet = "Content LIKE '%"+str+"%'"
    let foundTweet = (tweetsTable.Select(exprToFindTweet))
    let tweetRowSeq = seq { yield! foundTweet}
    let mutable tweetsList = []
    for x in tweetRowSeq do
        let userName = getUsernameFn(int(x.Field(tweetsTable.Columns.Item(3))))
        tweetsList  <- [int(x.Field(tweetsTable.Columns.Item(0))), userName, x.Field(tweetsTable.Columns.Item(1)).ToString()] |> List.append tweetsList
    tweetsList

let generateString (recordings: List<Performance>) =
    let mutable output = "UserId,FollowersOfUser,LiveUserCount,TimeTaken,IsRetweet\n"
    for i in recordings do
        output <- output + (sprintf "%d,%d,%d,%f,%b\n" i.UserId i.FollowersOfUser i.LiveUserCount i.TimeTaken i.IsRetweet)
    output

let server (systemRef:ActorSystem) (mailbox:Actor<_>) =

    let rec loop state = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | LoginOrSignup(userRef, userName) ->
            printfn "Login/Signup request received for user: %s" userName
            let userId = loginOrSignupFn(userName)
            userRef <! LoginDone(userId)
            return! loop {state with Users=state.Users.Add(userId, sender)}
        | Tweet(userId, userName, str) ->
            printfn "Tweet received for userId: %d with content %s" userId str
            let stopWatch = System.Diagnostics.Stopwatch.StartNew()
            let output = tweetFn(userId, userName, str)
            match output with 
            | (x, y, z) -> 
                let followers = getFollowersFn(userId)
                for x in followers do
                    if state.Users.ContainsKey(x) then
                        snd(state.Users.TryGetValue(x)) <! Update(output)
                sender <! TweetDone (x)
            stopWatch.Stop()
            return! loop {state with PerformanceRecording= state.PerformanceRecording @ [{UserId=userId; FollowersOfUser=getFollowersFn(userId).Length;LiveUserCount=state.Users.Count; TimeTaken=stopWatch.Elapsed.TotalMilliseconds;IsRetweet=false}]}
        | Follow(userRef, followerUserId, followingUserName) ->
            printfn "UserId %d wants to follow %s" followerUserId followingUserName
            let followed = followFn(followerUserId, followingUserName)
            userRef <! FollowDone (followed, followingUserName)
            return! loop state
        | Retweet(userId, tweetId) ->
            printfn "UserId %d wants to retweet tweetid %d" userId tweetId
            let stopWatch = System.Diagnostics.Stopwatch.StartNew()
            let output = retweetFn(userId, tweetId)
            match output with 
            | (x, y, z) -> 
                if x <> -1 then
                    let followers = getFollowersFn(userId)
                    for x in followers do
                        if state.Users.ContainsKey(x) then
                            snd(state.Users.TryGetValue(x)) <! Update(output)
                sender <! RetweetDone(output)
            stopWatch.Stop()
            return! loop {state with PerformanceRecording= state.PerformanceRecording @ [{UserId=userId; FollowersOfUser=getFollowersFn(userId).Length;LiveUserCount=state.Users.Count; TimeTaken=stopWatch.Elapsed.TotalMilliseconds;IsRetweet=true}]}
        | Timeline(userId) ->
            printfn "UserId %d wants to view timeline" userId 
            let followingUserIds = getFollowingFn(userId)
            let mutable result = []
            for x in followingUserIds do
                result <- getTweetByUserFn(x)  |> List.append result
            sender <! TimelineDone(result)
            return! loop state
        | Query(option, filter) ->
            printfn "Query request received for option %d filter %s" option filter
            if option = 1 then
                let userId = getUserIdFn(filter)
                let output = getTweetByUserFn(userId)
                sender <! QueryDone(output)
            else 
                let output = getTweetByPatternFn(filter)
                sender <! QueryDone(output)
            return! loop state
        | Logout (userId) ->
            printfn "Logout request received from UserId %d" userId
            sender <! LogoutDone
            return! loop ({state with Users=state.Users.Remove(userId)})
        | EndSimulation  (userId)-> 
            printfn "EndEmulation received from userId: %d" userId
            File.WriteAllText(@".\output.csv", generateString(state.PerformanceRecording))
            return! loop state
        | _ ->  failwith "Unknown message "
    }
    
    loop {Users = Map.empty; PerformanceRecording= List.empty}

let start () =
    let systemRef = System.create "Server" (configurationString serverPort)

    spawn systemRef "server" (server(systemRef)) 
    |> ignore
    
    systemRef.WhenTerminated.Wait()

// Code starts here
start()
