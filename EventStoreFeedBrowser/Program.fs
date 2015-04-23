open Akka
open Akka.FSharp
open Akka.Actor
open System.ServiceModel.Syndication
open System.Xml
open System.Net
open System.IO
open Akka.Routing

// Akka messages 
type PollMessage = 
    { Url : string
      PageProcessor : IActorRef }

type Entries = 
    { Entries : List<string> }

// ATOM helpers 
let getFeed (url : string) = 
    let reader = new XmlTextReader(url)
    let feed = SyndicationFeed.Load(reader)
    reader.Close()
    feed

let getEntriesUrls (feed : SyndicationFeed) = 
    feed.Items
    |> Seq.map (fun i -> i.Id)
    |> Seq.toList
    |> List.rev

let getFollowingPageUrl (feed : SyndicationFeed) = 
    let previous = 
        feed.Links
        |> Seq.filter (fun l -> l.RelationshipType = "previous")
        |> Seq.toList
    match previous.Length with
    | 0 -> None
    | _ -> Some(previous.Head.Uri.ToString())

// the actor system and its actors 
let system = ActorSystem.Create "MySystem"

let pageProcessor = 
    spawn system "pageProcessorNoPrints" <| fun mailbox -> 
        let rec loop() = 
            actor { 
                let! msg = mailbox.Receive()
                match msg with
                | { Entries = entries } -> printfn "%A" entries // to do something meaningful here 
                return! loop()
            }
        loop()

let pollByPage = 
    spawn system "pollPagesFromFeedNoPrints" <| fun mailbox -> 
        let rec loop() = 
            actor { 
                let! msg = mailbox.Receive()
                match msg with
                | { Url = url; PageProcessor = pageProcessor } -> 
                    let feed = getFeed (url)
                    let followingPage = getFollowingPageUrl (feed) // previous should be saved to DB when it's not null 
                    match followingPage with
                    | None -> // we tell ourselves to check for new entries in 1 second  
                        printfn "waiting..."
                        mailbox.Context.System.Scheduler.ScheduleTellOnce
                            (System.TimeSpan.FromSeconds(1.0), mailbox.Self, msg, mailbox.Self)
                    | Some(newUrl) -> 
                        let entries = getEntriesUrls (feed)
                        pageProcessor <! { Entries = entries } // send the entries to the processing actor 
                        mailbox.Self <! { msg with Url = newUrl } // tell ourselves to look at the next page 
                return! loop()
            }
        loop()

let pageProcessorWithPrints = 
    spawn system "pageProcessorWithPrints" <| fun mailbox -> 
        let rec loop() = 
            actor { 
                printfn "in page processor"
                let! msg = mailbox.Receive()
                printfn "message %A" msg
                match msg with
                | { Entries = entries } -> printfn "%A" entries
                return! loop()
            }
        loop()

// Use F# computation expression with tail-recursive loop 
// to create an actor message handler and return a reference 
let pollByPageWithPrints = 
    spawn system "pollPagesFromFeedWithPrints" <| fun mailbox -> 
        let rec loop() = 
            actor { 
                printfn "looping "
                let! msg = mailbox.Receive()
                printfn "message %A" msg
                match msg with
                | { Url = url; PageProcessor = pageProcessor } -> 
                    let feed = getFeed (url)
                    printfn "Hello, %A!" url
                    let followingPage = getFollowingPageUrl (feed)
                    printfn "Following page %A" followingPage
                    match followingPage with
                    | None -> 
                        printfn "no new items for %A" url
                        // we send a message to ourselves in 1 second
                        mailbox.Context.System.Scheduler.ScheduleTellOnce
                            (System.TimeSpan.FromSeconds(1.0), mailbox.Self, msg, mailbox.Self)
                    | Some(newUrl) -> 
                        printfn "new items available, starting at %A" newUrl
                        let entries = getEntriesUrls (feed)
                        printfn "Entries in pollPagesFromFeed %A" entries
                        pageProcessor <! { Entries = entries }
                        mailbox.Self <! { msg with Url = newUrl }
                return! loop()
            }
        loop()

[<EntryPoint>]
let main argv = 
    // pass here the link to the oldest unread page (which is that thing saved in the DB)
    pollByPage <! { Url = "http://localhost:2113/streams/test/0/forward/20"
                    PageProcessor = pageProcessor }
    // uncomment this to see what the sequence of messages really is (loads of print messages!)
//    pollByPageWithPrints <! { Url = "http://localhost:2113/streams/test/0/forward/20"
//                              PageProcessor = pageProcessorWithPrints }
    system.AwaitTermination()
    0
