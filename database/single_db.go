package database

func genExpireTask(key string) string {
	return "expire:" + key
}
