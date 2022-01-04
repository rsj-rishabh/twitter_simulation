## About
Distributed simulation of twitter.

## How to run

Engine:

`dotnet fsi Server.fsx"`

Client:

`dotnet fsi Client.fsx <no. of users> <IP address of the server>`

Example: `dotnet fsi Client.fsx 1000 10.20.244.195`

## Dependencies

1. FSharp
2. [Akka.NET](https://www.nuget.org/packages/Akka/)
3. [MathNet.Numerics](https://www.nuget.org/packages/MathNet.Numerics/) for Zipf distribution

## What is working
-	The client processes were run on a single machine as actors and the engine process was run on a different machine (again using actor model) considering the load it has to handle. (That is, client processes and engine are separate processes)
-	Users can register, sign in, and sign out.
-	Users can follow another user.
-	Users can tweet/retweet.
-	Users can receive live updates without querying.
-	Users can view timeline (tweets/retweets of users they followed), query based on hashtags and mentions.
-	We distributed number of subscribers in a Zipf distribution and recorded results for networks with 1000 users and 10,000 users.


## Team Members
- Siddhesh P Patil (siddheshpatil@ufl.edu)
- Rishabh Jaiswal (rishabh.jaiswal@ufl.edu)