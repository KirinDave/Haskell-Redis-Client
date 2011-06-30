module Test where

import System (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.Environment
import System.Console.GetOpt
import Data.Maybe (isJust, isNothing)
import Control.Monad (when)

import Test.Setup
import qualified Test.Connection as Connection
import qualified Test.GenericCommands as GenericCommands
import qualified Test.StringCommands as StringCommands
import qualified Test.ListCommands as ListCommands
import qualified Test.SetCommands as SetCommands
import qualified Test.ZSetCommands as ZSetCommands
import qualified Test.HashCommands as HashCommands
import qualified Test.SortCommands as SortCommands
import qualified Test.PubSubCommands as PubSubCommands
import qualified Test.MultiCommands as MultiCommands
import qualified Test.CASCommands as CASCommands
import qualified Test.Monad.MultiCommands as M_MultiCommands
import qualified Test.Monad.CASCommands as M_CASCommands
import qualified Test.Lock as Lock

tests = [("connection", TLabel "Connection" Connection.tests),
         ("generic", TLabel "Generic commands" GenericCommands.tests),
         ("string", TLabel "String commands" StringCommands.tests),
         ("list", TLabel "List commands" ListCommands.tests),
         ("set", TLabel "Set commands" SetCommands.tests),
         ("zset", TLabel "Sorted set commands" ZSetCommands.tests),
         ("hash", TLabel "Hash commands" HashCommands.tests),
         ("sort", TLabel "Sort comands" SortCommands.tests),
         ("pubsub", TLabel "Pub/Sub commands" PubSubCommands.tests),
         ("multi", TLabel "Multi commands" MultiCommands.tests),
         ("cas", TLabel "CAS commands" CASCommands.tests),
         ("multi", TLabel "Multi commands within monad wrapper" M_MultiCommands.tests),
         ("cas", TLabel "CAS commands within monad wrapper" M_CASCommands.tests),
         ("lock", TLabel "Lock" Lock.tests)]

mkTests Nothing = TList $ map snd tests
mkTests (Just label) = TList $ map snd $ filter ((== label) . fst) tests

defaultOpts :: Opts
defaultOpts = Opts localhost defaultPort 0 Nothing Nothing False

options :: [OptDescr (Opts -> Opts)]
options = [Option ['h'] ["host"] (OptArg (maybe id (\h o -> o{optHost = h})) "HOSTNAME")
                      ("Server hostname (default " ++ localhost ++ ")"),
           Option ['p'] ["port"] (OptArg (maybe id (\p o -> o{optPort = p})) "PORT")
                      ("Server port (default " ++ defaultPort ++ ")"),
           Option ['d'] ["database"] (OptArg (maybe id (\d o -> o{optDatabase = read d})) "DATABASE")
                      "Database number (default 0)",
           Option ['b'] ["binary"] (OptArg (maybe id (\b o -> o{optBinary = Just b})) "PATH")
                      "Redis server binary (start this binary for running tests on it)",
           Option ['c'] ["config"] (OptArg (maybe id (\c o -> o{optConfig = Just c})) "PATH")
                      "Config file (used with \"binary\" option)",
           Option [] ["help"] (NoArg (\o -> o{optHelp = True})) "Show this usage info"]

main :: IO ()
main = do args <- getArgs

          (opts, label) <- case getOpt RequireOrder options args of
                             (o, l, []) -> let opts = foldl (flip id) defaultOpts o
                                               label | null l = Nothing
                                                     | otherwise = Just $ head l
                                           in return (opts, label)
                             (_, _, es) -> do putStrLn $ concat es ++ usageInfo "\nUsage:" options
                                              exitFailure

          when (optHelp opts) $ do putStrLn $ usageInfo "Usage: " options
                                   exitSuccess

          when (isJust (optBinary opts) && isNothing (optConfig opts))
                   $ do putStrLn "Config file must be specified when used with \"binary\" option"
                        putStrLn $ usageInfo "\nUsage: " options
                        exitFailure

          when (isJust (optBinary opts)) $ startRedis (fromJust $ optBinary opts) (fromJust $ optConfig opts)

          runTestTT $ toHUnit $ pushParam opts $ mkTests label
          when (isJust (optBinary opts)) shutdownRedis
