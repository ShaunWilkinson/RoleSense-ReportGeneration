const mssql = require('mssql')
const pools = new Map();

module.exports = {
	async get(name, config) {
		if(!pools.has(name)) {
			if(!config) {
				throw new Error('Pool does not exist');
			}

			const pool = new mssql.ConnectionPool(config);

			// automatically remove the pool from the cache if `pool.close()` is called
			const close = pool.close.bind(pool);
			pool.close = (...args) => {
				pools.delete(name);
				return close(...args);
			}

			pools.set(name, pool.connect());
		}

		return pools.get(name);
	},

	/**
	 * Closes all the pools and removes them from the store
	 *
	 * @return {Promise<mssql.ConnectionPool[]>}
	 */
	async closeAll() {
		Promise.all(Array.from(pools.values()).map((connect) => {
			return connect.then((pool) => pool.close());
		}))
	}
}