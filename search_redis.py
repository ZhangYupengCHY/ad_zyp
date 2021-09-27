from my_toolkit import public_function
import json



redisConn = public_function.Redis_Store(db=2)
fiveSignKey = 'FIVE_FILES_KEYS_SAVE'
redisDb2keys = redisConn.keys()
fiveFilesKey = [key for key in redisDb2keys if fiveSignKey in key]
for key in fiveFilesKey:
    station = key[21:-18]
    stationType = key[-17:-15].lower()
    # path = r"F:\pickle_files\02_AU_ac.pkl"
    path = f"G:\pickle_files\{station}_{stationType}.pkl"
    redisConn.set(key,path)

