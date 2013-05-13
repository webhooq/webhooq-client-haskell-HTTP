module Network.HTTP.Webhooq
  ( declareExchange
  , deleteExchange
  , declareQueue
  , deleteQueue
  , bindExchange
  , bindQueue
  , publish
  , ExchangeType (..)
  , Link(..)
  , LinkValue(..)
  , WebhooqServer(..)

  , LinkParam
  , WebhooqResponse
  , ExchangeName
  , SourceExchangeName
  , DestinationExchangeName
  , QueueName
  , Argument
  , RoutingKey
  , ResponseReason

  , header_exchange
  , header_queue
  , header_routing_key
  , header_link
  , header_message_id

  , mkHostHeader
  , mkExchangeHeader
  , mkQueueHeader
  , mkRoutingKeyHeader
  , mkLinkHeader
  , mkMessageIdHeader

  , show --Show (Link, LinkValue, ExchangeType)
 ) where

import Network.HTTP
import Network.URI
import Network.Stream

import qualified Codec.MIME.Type as Mime

import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8

import Data.Text (Text)
import qualified Data.Text as Txt

data ExchangeType 
  = Direct
  | Fanout
  | Topic
instance Show ExchangeType where 
  show Direct = "direct"
  show Fanout = "fanout"
  show Topic  = "topic"

data Link = Link [LinkValue]
data LinkValue = LinkValue URI [LinkParam]
type LinkParam = (Text,Text)
instance Show Link where
  show (Link lvs) = 
    Txt.unpack $ Txt.intercalate (Txt.pack ",") (map (Txt.pack . show) lvs)

instance Show LinkValue where
  show (LinkValue uri lps) = 
    Txt.unpack $ Txt.concat 
      [ Txt.pack "<"
      , Txt.pack (show uri)
      , Txt.pack ">"
      , Txt.concat (map showLinkParam lps)
      ]
    where
    showLinkParam (k,v) = 
      Txt.concat 
        [ Txt.pack "; "
        , k
        , Txt.pack "=\""
        , v
        , Txt.pack "\""
        ]  

data WebhooqServer = WebhooqServer
  { wqBaseURI :: URI
 -- , wqHost :: String
  } deriving (Show)

type WebhooqResponse = Either String () 
type ExchangeName    = String 
type SourceExchangeName         = String 
type DestinationExchangeName    = String 
type QueueName       = String 
type Argument        = (String, String)
type RoutingKey      = String
type ResponseReason  = String

http_created         = (2,0,1)
http_no_content      = (2,0,4)
http_accepted        = (2,0,2)

shoh :: ResponseCode -> String 
shoh (x, y, z) = (show x)++(show y)++(show z)

webhooq :: URI -> [Header] -> RequestMethod -> Maybe (String,String) -> ResponseCode -> IO (ResponseCode,ResponseReason)
webhooq uri headers method body expectedResponseCode = do
  let request'' = mkRequest method uri
  let request'  = maybe request'' (setRequestBody request'') body 
  let request   = setHeaders request' headers
  response       <- simpleHTTP request :: IO (Result (Response String)) 
  responseCode   <- getResponseCode   response
  responseReason <- getResponseReason response
  if responseCode == expectedResponseCode 
    then return (responseCode, responseReason)
    else fail ("'"++(show uri)++"' did not receive expected "++(shoh expectedResponseCode)++" response code! Received: "++(shoh responseCode)++" "++responseReason)

declareExchange :: WebhooqServer -> [Header] -> ExchangeType -> ExchangeName -> [Argument] -> IO () 
declareExchange server headers exchange_type exchange_name arguments = do
  let path  = "/exchange/" ++ exchange_name
  let query = "?"++(a2qs $ ("type", show exchange_type) : arguments)
  let uri   = (wqBaseURI server) { uriPath = path, uriQuery = query }
  let hdrs  = headers
  webhooq uri hdrs POST Nothing http_created
  return ()

deleteExchange :: WebhooqServer -> [Header] -> ExchangeName -> IO () 
deleteExchange server headers exchange_name = do
  let path  = "/exchange/" ++ exchange_name
  let uri   = (wqBaseURI server) { uriPath = path }
  let hdrs  = headers
  webhooq uri hdrs DELETE Nothing http_no_content
  return ()

declareQueue :: WebhooqServer -> [Header] -> QueueName -> [Argument] -> IO () 
declareQueue server headers queue_name arguments = do
  let path  = "/queue/" ++ queue_name
  let query = "?"++(a2qs arguments)
  let uri   = (wqBaseURI server) { uriPath = path, uriQuery = query }
  let hdrs  = headers
  webhooq uri hdrs POST Nothing http_created
  return ()

deleteQueue :: WebhooqServer -> [Header] -> QueueName -> IO () 
deleteQueue server headers queue_name = do
  let path  = "/queue/" ++ queue_name
  let uri   = (wqBaseURI server) { uriPath = path }
  let hdrs  = headers
  webhooq uri hdrs DELETE Nothing http_no_content
  return ()

bindExchange :: WebhooqServer -> [Header] -> SourceExchangeName -> RoutingKey -> DestinationExchangeName -> IO ()
bindExchange server headers source_exchange routing_key destination_exchange = do
  let path = "/exchange/" ++ source_exchange ++ "/bind"
  let uri   = (wqBaseURI server) { uriPath = path }
  let hdrs  = (mkExchangeHeader destination_exchange) : (mkRoutingKeyHeader routing_key) : headers
  webhooq uri hdrs POST Nothing http_created
  return ()

bindQueue :: WebhooqServer -> [Header] -> SourceExchangeName -> RoutingKey -> QueueName -> Link -> IO ()
bindQueue server headers source_exchange routing_key queue_name link = do
  let path = "/exchange/" ++ source_exchange ++ "/bind"
  let uri   = (wqBaseURI server) { uriPath = path }
  let hdrs  = (mkLinkHeader link) : (mkQueueHeader queue_name) : (mkRoutingKeyHeader routing_key) : headers
  webhooq uri hdrs POST Nothing http_created
  return ()

publish :: WebhooqServer -> [Header] -> ExchangeName -> RoutingKey -> Mime.Type -> ByteString -> IO()
publish server headers exchange_name routing_key mime message = do
  let path  = "/exchange/" ++ exchange_name ++ "/publish"
  let uri   = (wqBaseURI server) { uriPath = path }
  let hdrs  = (mkRoutingKeyHeader routing_key) : headers
  let ctype = Mime.showType mime 
  let body  = B8.unpack message 
  webhooq uri hdrs POST (Just (ctype,body)) http_accepted
  return ()


------------------------------------------------------------
header_exchange     = HdrCustom "x-wq-exchange"    -- exchange_name
header_queue        = HdrCustom "x-wq-queue"       -- queue_name
header_routing_key  = HdrCustom "x-wq-routing-key" -- an AMQP routing key
header_link         = HdrCustom "x-wq-link"        -- an RFC-5988 Link Header value
header_message_id   = HdrCustom "x-wq-msg-id"      -- UUID
------------------------------------------------------------
mkHostHeader host              = Header HdrHost host
mkExchangeHeader exchange_name = Header header_exchange exchange_name
mkQueueHeader queue_name       = Header header_queue queue_name
mkRoutingKeyHeader routing_key = Header header_routing_key routing_key
mkLinkHeader link              = Header header_link (show link)
mkMessageIdHeader message_id   = Header header_message_id message_id
------------------------------------------------------------
a2qs :: [Argument] -> String
a2qs = Txt.unpack . (Txt.intercalate (Txt.pack "&")) . (map param)
  where
  param (k,v) = Txt.concat [Txt.pack k, Txt.pack "=", Txt.pack v]

getResponseReason :: Result (Response ty) -> IO String 
getResponseReason (Left err) = fail (show err)
getResponseReason (Right r)  = return (rspReason r)
