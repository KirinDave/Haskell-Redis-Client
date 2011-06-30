module Database.Redis.Info (
    RedisInfo, parseInfo
) where

import Text.Parsec
import Data.Map

import Database.Redis.ByteStringClass

type RedisInfo = Map String String

parseInfo :: String -> Either ParseError RedisInfo
parseInfo = runParser infoP empty "info"
    
infoP :: Parsec String RedisInfo RedisInfo
infoP = do skipMany infoLine
           getState

infoLine :: Parsec String RedisInfo ()
infoLine = emptyLine  <|> commentLine <|> keyLine

emptyLine :: Parsec String RedisInfo ()
emptyLine = skipMany (oneOf [' ', '\t', '\r']) >> newline >> return ()

commentLine :: Parsec String RedisInfo ()
commentLine = char '#' >> skipMany (noneOf ['\n']) >> newline >> return ()

keyLine :: Parsec String RedisInfo ()
keyLine = do key <- many1 (noneOf [':'])
             char ':'
             val <- many1 (noneOf [' ', '\t', '\r', '\n'])
             skipMany (oneOf [' ', '\t', '\r'])
             newline
             m <- getState
             setState $ insert key val m
