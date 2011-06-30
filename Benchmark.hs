import System (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.Console.GetOpt
import Data.Time.Clock
import Database.Redis.Redis
import Control.Concurrent.MVar
import Control.Concurrent
import Control.Monad (when)

makeConnections :: Int -> String -> String -> Int -> IO [Redis]
makeConnections count host port db = mapM mkConn [0 .. (count - 1)]
    where mkConn n = do r <- connect host port
                        select r db
                        return r

-- Worker :: Redis -> worker prefix -> loop number -> lock -> unlock -> IO ()
type Worker = Redis -> String -> Int -> MVar () -> MVar () -> IO ()

-- runWorkers :: Redis -> total loops count -> Worker -> IO time passed
runWorkers :: [Redis] -> Int -> Worker -> IO NominalDiffTime
runWorkers rs loops worker =
    do mvs <- mapM fork $ zip [0 .. (workers - 1)] rs
       t <- getCurrentTime
       mapM_ (flip putMVar () . fst) mvs
       mapM_ (takeMVar . snd ) mvs
       flip diffUTCTime t `fmap` getCurrentTime
    where fork (n, r) = do lock <- newEmptyMVar
                           unlock <- newEmptyMVar
                           if n < workers - 1
                             then forkIO $ worker r (prefix n) count lock unlock
                             else forkIO $ worker r (prefix n) (loops - (count * (workers - 1))) lock unlock
                           return (lock, unlock)
          count = loops `quot` (workers - 1)
          workers = length rs
          prefix n = (show n) ++ ":"

printResult name loops t = do putStrLn $ name ++ ": " ++ (show t)
                              putStrLn $ (show $ (fromIntegral loops) / t) ++ " per second"

worker_set :: Worker
worker_set r x n l u = do takeMVar l
                          loop sets
                          putMVar u ()
    where sets = zip keys vals
          keys = map ((x ++) . show) [1..n]
          vals = [1..n]
          loop [] = return ()
          loop ((k, v) : s) = set r k v >> loop s

worker_get :: Worker
worker_get r x n l u = do takeMVar l
                          loop keys
                          putMVar u ()
    where keys = map ((x ++) . show) [1..n]
          loop [] = return ()
          loop (k:s) = (get r k :: IO (Reply ())) >> loop s

worker_lpush :: Worker
worker_lpush r x n l u = do takeMVar l
                            loop vals
                            putMVar u ()
    where vals = [1..n]
          key = x ++ "lst"
          loop = mapM_ (lpush r key)

worker_lpop :: Worker
worker_lpop r x n l u = do takeMVar l
                           loop n
                           putMVar u ()
    where key = x ++ "lst"
          loop n = mapM_ (\_ -> lpop r key :: IO (Reply ())) [1..n]

data Opt = Opt { optHost     :: String,
                 optPort     :: String,
                 optDatabase :: Int,
                 optClients  :: Int,
                 optRequests :: Int,
                 optHelp     :: Bool}
           deriving Show

defaultOpts = Opt localhost defaultPort 6 50 100000 False

options :: [OptDescr (Opt -> Opt)]
options = [Option ['h'] ["host"] (OptArg (maybe id (\h o -> o{optHost = h})) "HOSTNAME") ("Server hostname (default " ++ localhost ++ ")"),
           Option ['p'] ["port"] (OptArg (maybe id (\p o -> o{optPort = p})) "PORT") ("Server port (default " ++ defaultPort ++ ")"),
           Option ['d'] ["database"] (OptArg (maybe id (\d o -> o{optDatabase = read d})) "DATABASE") "Database number (default 6)",
           Option ['c'] ["clients"] (OptArg (maybe id (\c o -> o{optClients = read c})) "CLIENTS") "Number of parallel connections (default 50)",
           Option ['n'] ["requests"] (OptArg (maybe id (\n o -> o{optRequests = read n})) "REQUESTS") "Total number of requests (default 100000)",
           Option [] ["help"] (NoArg (\o -> o{optHelp = True})) "Show this usage info"]

main :: IO ()
main = do args <- getArgs
          opts <- case getOpt RequireOrder options args of
                    (o, [], []) -> return $ foldl (flip id) defaultOpts o
                    (_, n, [])  -> do putStrLn $ "Unrecognized arguments: " ++ concat n ++ usageInfo "\nUsage: " options
                                      exitFailure
                    (_, _, es)  -> do putStrLn $ concat es ++ usageInfo "\nUsage:" options
                                      exitFailure

          when (optHelp opts) $ do putStrLn $ usageInfo "Usage:" options
                                   exitSuccess

          r <- connect (optHost opts) (optPort opts)
          select r $ optDatabase opts
          flushDb r

          rs <- makeConnections (optClients opts) (optHost opts) (optPort opts) (optDatabase opts)

          t <- runWorkers rs (optRequests opts) worker_set
          printResult "set" (optRequests opts) t

          t <- runWorkers rs (optRequests opts) worker_get
          printResult "get" (optRequests opts) t

          t <- runWorkers rs (optRequests opts) worker_lpush
          printResult "lpush" (optRequests opts) t

          t <- runWorkers rs (optRequests opts) worker_lpop
          printResult "lpop" (optRequests opts) t

          putStrLn "Done."