const db = {};

require('dotenv').config()

db.sqlConfig = {
	user: process.env.AZURE_SQL_USERNAME,
	password: process.env.AZURE_SQL_PASSWORD,
	database: process.env.AZURE_SQL_DATABASE,
	server: process.env.AZURE_SQL_SERVER,
	requestTimeout: 30000,
	pool: {
	  max: 20,
	  min: 0,
	  idleTimeoutMillis: 30000
	},
	dialectOptions: {
	  options: {
		requestTimeout: 30000
	  }
	},
	options: {
	  encrypt: true,
	  trustServerCertificate: true
	}
  }

module.exports = db;