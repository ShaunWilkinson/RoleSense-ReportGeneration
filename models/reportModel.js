// @ts-check
const sql = require("mssql")
const {sqlConfig} = require('../config/db')
const {get} = require('../controllers/sqlPoolManager')

class ReportModel {
    async getWorkspaceId(tenantId) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("tenantId", sql.UniqueIdentifier, tenantId)
			.query(`
				SELECT TOP(1) logAnalyticsWorkspaceId
				FROM Licence
				WHERE tenantId = @tenantId
			`)
			.catch(err => {
				console.error("Failed to getWorkspaceId")
				console.error(err)
			})
	
		return result?.recordset?.[0]?.logAnalyticsWorkspaceId ?? null;
	}
	
	async getReport(reportId) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("reportId", sql.UniqueIdentifier, reportId)
			.query(`
				SELECT TOP(1) id, tenantId, subscriptionId, resourceGroupName, resourceGroupId, fromDate
				FROM Reports
				WHERE id = @reportId
			`)
			.catch(err => {
				console.error("Failed to getReport")
				console.error(err)
			})
	
		return result?.recordset?.[0] ?? null;
	}

	async getPIMReport(reportId) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("reportId", sql.UniqueIdentifier, reportId)
			.query(`
				SELECT TOP(1) id, tenantId, fromDate
				FROM ReportsPIM
				WHERE id = @reportId
			`)
			.catch(err => {
				console.error("Failed to getPIMReport")
				console.error(err)
			})
	
		return result?.recordset?.[0] ?? null;
	}

	async updateReportStatus(reportId, statusId) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("reportId", sql.UniqueIdentifier, reportId)
			.input("statusId", sql.Int, statusId)
			.query(`
				UPDATE Reports
				SET status = @statusId
				WHERE id = @reportId
			`)
			.catch(err => {
				console.error("Failed to updateReportStatus")
				console.error(err)
			})
	
		return result?.rowsAffected?.[0] > 0;
	}

	async updatePIMReportStatus(reportId, statusId) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("reportId", sql.UniqueIdentifier, reportId)
			.input("statusId", sql.Int, statusId)
			.query(`
				UPDATE ReportsPIM
				SET status = @statusId
				WHERE id = @reportId
			`)
			.catch(err => {
				console.error("Failed to updatePIMReportStatus")
				console.error(err)
			})
	
		return result?.rowsAffected?.[0] > 0;
	}

	async updateDataSource(reportId, dataSource) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("reportId", sql.UniqueIdentifier, reportId)
			.input("dataSource", sql.VarChar(120), dataSource)
			.query(`
				UPDATE Reports
				SET dataSource = @dataSource
				WHERE id = @reportId
			`)
			.catch(err => {
				console.error("Failed to updateDataSource")
				console.error(err)
			})
	
		return result?.rowsAffected?.[0] > 0;
	}

	async updatePIMDataSource(reportId, dataSource) {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.input("reportId", sql.UniqueIdentifier, reportId)
			.input("dataSource", sql.VarChar(120), dataSource)
			.query(`
				UPDATE ReportsPIM
				SET dataSource = @dataSource
				WHERE id = @reportId
			`)
			.catch(err => {
				console.error("Failed to updatePIMDataSource")
				console.error(err)
			})
	
		return result?.rowsAffected?.[0] > 0;
	}

	async getOldestRequestedReport() {
		let pool = await get('default', sqlConfig);
		let result = await pool.request()
			.query(`
				SELECT TOP(1) id, tenantId, subscriptionId, resourceGroupName, resourceGroupId
				FROM Reports
				WHERE status = 1
				ORDER BY created DESC

			`)
			.catch(err => {
				console.error("Failed to getOldestRequestedReport")
				console.error(err)
			})
	
		return result?.recordset?.[0] ?? null;
	}

	async bulkSaveRoleSuggestions(reportId, roleSuggestions) {
		if (roleSuggestions.length === 0) return; // No data to insert

		let pool;
		try {
			pool = await get('default', sqlConfig);

			// Step 2: Generate bulk insert query
			const columns = Object.keys(roleSuggestions[0]);
			const values = roleSuggestions.map(row => 
				`('${reportId}', ${columns.map(column => `'${row[column]}'`).join(", ")})`
			).join(", ");

			const query = `
				INSERT INTO RoleSuggestions (reportId, ${columns.join(", ")})
				VALUES ${values}
			`;

			// Step 3: Execute the query
			await pool.request().query(query);
			console.log('Bulk insert successful');
			return true;
		} catch (err) {
			console.error("Failed to perform bulk insert");
			console.error(err);
			return false;
		}
	}
	
	async bulkSavePIMSuggestions(reportId, suggestions) {
		if (suggestions.length === 0) return; // No data to insert

		let pool;
		try {
			pool = await get('default', sqlConfig);

			// Step 2: Generate bulk insert query
			const columns = Object.keys(suggestions[0]);
			const escapeValue = (value) => {
				if (value === null || value === undefined) {
					return 'NULL'; // Handle null or undefined as NULL
				}
				if (typeof value === 'boolean') {
					return value ? 1 : 0; // Convert boolean to 1 or 0
				}
				if (typeof value === 'number') {
					return value; // Numbers are inserted as-is
				}
				if (typeof value === 'string') {
					return `'${value.replace(/'/g, "''")}'`; // Escape single quotes in strings
				}
				throw new Error(`Unsupported data type: ${typeof value}`);
			};
			const values = suggestions.map(row => 
				`('${reportId}', ${columns.map(column => escapeValue(row[column])).join(", ")})`
				).join(", ");

			const query = `
				INSERT INTO PIMSuggestions (reportId, ${columns.join(", ")})
				VALUES ${values}
			`;

			// Step 3: Execute the query
			await pool.request().query(query);
			console.log('Bulk insert successful');
			return true;
		} catch (err) {
			console.error("Failed to perform bulk insert");
			console.error(err);
			return false;
		}
	}
}

module.exports = ReportModel