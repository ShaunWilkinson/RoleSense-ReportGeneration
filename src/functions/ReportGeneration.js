const { app } = require('@azure/functions');
const { QueueClient } = require('@azure/storage-queue');
const { ClientSecretCredential } = require('@azure/identity');
const axios = require('axios');
const connectionString = process.env.reportqueue_STORAGE;

app.storageQueue('ReportGeneration', {
    queueName: 'report-requests',
    connection: 'reportqueue_STORAGE',
    handler: async (queueItem, context) => {
        context.log('Storage queue function processed work item:', queueItem);

		try {
			await generateReport(queueItem)
		} catch (error) {
			console.error('Error processing report, re-adding to queue:', error);

            // Re-add the failed report to the queue
            const queueClient = new QueueClient(connectionString, 'report-requests');
            await queueClient.sendMessage(Buffer.from(JSON.stringify(queueItem)).toString('base64'));

            console.log('Re-added report to queue for retry:', queueItem);
		}
    }
});

app.storageQueue('PIMReportGeneration', {
    queueName: 'pim-report-requests',
    connection: 'reportqueue_STORAGE',
    handler: async (queueItem, context) => {
        context.log('Storage queue function processed work item:', queueItem);

		try {
			await generatePIMReport(queueItem)
		} catch (error) {
			console.error('Error processing PIM report, re-adding to queue:', error);

            // Re-add the failed report to the queue
            const queueClient = new QueueClient(connectionString, 'pim-report-requests');
            await queueClient.sendMessage(Buffer.from(JSON.stringify(queueItem)).toString('base64'));

            console.log('Re-added PIM report to queue for retry:', queueItem);
		}
    }
});

const { getAzureFormattedTimestamp } = require('../../controllers/utilities');
const ReportModel = require('../../models/reportModel')
const reportModel = new ReportModel();

async function getAccessToken(tenantId, scope) {
	if(!tenantId || !scope) return "Missing params"
	const credential = new ClientSecretCredential(tenantId, process.env.AZURE_CLIENT_ID, process.env.AZURE_CLIENT_SECRET)
	const token = await credential.getToken(scope);
	return token.token;
}

async function generateReport(queueItem) {
	var reportId = queueItem.reportId;
	const requestedReport = await reportModel.getReport(reportId);
	if(!requestedReport) {
		console.log("No requested reports found")
		return ;
	}

	try {

		await axios.post(`https://rolesense.org/report/updateReportStatus?id=${requestedReport.id}&status=Processing`)
		await reportModel.updateReportStatus(requestedReport.id, 2) // Set the report status as processing

		console.log(`Report ${reportId} - Started data collection for report`)
		const tenantId = requestedReport.tenantId;

		// Get if workspaceId there
		const workspaceId = await reportModel.getWorkspaceId(tenantId)
		var oldestRecord = null, useWorkspace = false;
		if(workspaceId) {
			oldestRecord = await retrieveActivityLogs(tenantId, workspaceId, 1, requestedReport.subscriptionId) // Get oldest workspace log

			if(oldestRecord && oldestRecord.length == 0) {
				console.warn(`No records detected in workspace with id ${workspaceId}`)
				useWorkspace = false;
			} else {
				let oldestWorkspaceLog = new Date(oldestRecord[0].TimeGenerated)
				let thirtyDaysAgo = new Date();
				thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
	
				// Compare the parsed date with 30 days ago
				if (oldestWorkspaceLog <= thirtyDaysAgo) {
					useWorkspace = true;
				}
			}
		} 

		await reportModel.updateDataSource(requestedReport.id, 'Workspace')
		
		// Get required data
		if(!useWorkspace) {
			var [activityLogs, roleDefinitions, roleAssignments, userDetails, servicePrincipals] = await Promise.all([
				retrieveActivityLogs(tenantId, requestedReport.subscriptionId, requestedReport.resourceGroupName, requestedReport.fromDate),
				listRoleDefinitions(tenantId, requestedReport.subscriptionId, requestedReport.resourceGroupId, true),
				listRoleAssignments(tenantId, requestedReport.subscriptionId, requestedReport.resourceGroupId),
				listUsers(tenantId, true),
				listServicePrincipals(tenantId, true)
			])
		} else {
			var [activityLogs, roleDefinitions, roleAssignments, userDetails, servicePrincipals] = await Promise.all([
				retrieveActivityLogs(tenantId, workspaceId, null, requestedReport.subscriptionId, requestedReport.fromDate),
				listRoleDefinitions(tenantId, requestedReport.subscriptionId, requestedReport.resourceGroupId, true),
				listRoleAssignments(tenantId, requestedReport.subscriptionId, requestedReport.resourceGroupId),
				listUsers(tenantId, true),
				listServicePrincipals(tenantId, true)
			])
		}

		if(!activityLogs) {
			console.error("Failed to retrieve audit logs", tenantId, workspaceId)
		}

		var allAccountDetails = userDetails;
		allAccountDetails = allAccountDetails.concat(servicePrincipals)

		roleAssignments = roleAssignments.map(assignment => assignment.properties) // Retrieve just the properties property of each assignment
		const roleAssignmentPrincipalIds = [...new Set(roleAssignments.map(assignment => assignment["principalId"]))] // Retrieve an array of unique users from the logs

		// Loop through each identified user and verify if the roles are needed
		let principalId, usersRoleAssignments, relatedActivityLogs, requiredActions, roleSuggestions = [], user;
		for(let i=0; i<roleAssignmentPrincipalIds.length; i++) {
			console.log(`Report ${requestedReport.id} - Processing user ${i+1} of ${roleAssignmentPrincipalIds.length}`)

			principalId = roleAssignmentPrincipalIds[i]
			user = allAccountDetails.find(user => user.id == principalId)

			usersRoleAssignments = roleAssignments
				.filter(assignment => assignment.principalId == principalId) // roles assigned to the given user

			if(user && user.userPrincipalName) {
				relatedActivityLogs = activityLogs.filter(log => log.caller == principalId || log.caller == user.userPrincipalName)
			} else {
				relatedActivityLogs = activityLogs.filter(log => log.caller == principalId)
			}

			if(useWorkspace) {
				if(relatedActivityLogs && relatedActivityLogs.length > 0) {
					requiredActions = [...new Set(relatedActivityLogs.map(log => JSON.parse(log?.Authorization)?.action))]
				} else {
					requiredActions = [];
				}
			} else {
				if(relatedActivityLogs && relatedActivityLogs.length > 0) {
					requiredActions = [...new Set(relatedActivityLogs.map(log => log?.["authorization"]?.action ?? false))]
				} else {
					requiredActions = [];
				}
			}

			// Process all of the roles assigned to a given user
			usersRoleAssignments.forEach((roleAssignment) => {
				roleSuggestions.push(getRoleSuggestion(roleAssignment, roleDefinitions, requiredActions, allAccountDetails));
			})
		}

		// Save the role report
		console.log(`Report ${requestedReport.id} - Saving suggested roles to Database`)
		const result = await reportModel.bulkSaveRoleSuggestions(requestedReport.id, roleSuggestions)

		await axios.post(`https://rolesense.org/report/updateReportStatus?id=${requestedReport.id}&status=Ready`)
		await reportModel.updateReportStatus(requestedReport.id, 3) // Set the report status as ready
		console.log(result);
	} catch (error) {
		console.error("Failed to process report");
		console.error(error);
		await axios.post(`https://rolesense.org/report/updateReportStatus?id=${requestedReport.id}&status=Failed`)
		await reportModel.updateReportStatus(requestedReport.id, 4) // Set the report status as processing
		throw error
	}
}

async function generatePIMReport(queueItem) {
	var reportId = queueItem.reportId;
	const requestedReport = await reportModel.getPIMReport(reportId);
	if(!requestedReport) {
		console.log("No requested reports found")
		return ;
	}

	try {

		await axios.post(`https://rolesense.org/report/updateReportStatus?id=${requestedReport.id}&status=Processing`)
		await reportModel.updatePIMReportStatus(requestedReport.id, 2) // Set the report status as processing

		console.log(`PIM Report ${reportId} - Started data collection for report`)
		const tenantId = requestedReport.tenantId;

		// Get if workspaceId there
		const workspaceId = await reportModel.getWorkspaceId(tenantId)
		var oldestRecord = null, useWorkspace = false;
		if(workspaceId) {
			oldestRecord = await retrieveAuditLogsFromWorkspace(tenantId, workspaceId, 1) // Get oldest workspace log

			if(oldestRecord && oldestRecord.length == 0) {
				console.warn(`No AuditLog records detected in workspace with id ${workspaceId}`)
				useWorkspace = false;
			} else {
				let oldestWorkspaceLog = new Date(oldestRecord[0].TimeGenerated)
				let thirtyDaysAgo = new Date();
				thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
	
				// Compare the parsed date with 30 days ago
				if (oldestWorkspaceLog <= thirtyDaysAgo) {
					useWorkspace = true;
				}
			}
		} 

		await reportModel.updateDataSource(requestedReport.id, 'Workspace')
		
		// Get required data
		if(!useWorkspace) {
			var [auditLogs, eligibleRoles, userDetails, servicePrincipals, groups, roleDefinitions] = await Promise.all([
				retrieveAuditLogsFromWorkspace(tenantId, workspaceId, 9999999, requestedReport.fromDate),
				listEligibleRoles(tenantId),
				listUsers(tenantId, true),
				listServicePrincipals(tenantId, true),
				listGroups(tenantId, true),
				listEntraIdRoles(tenantId)
			])
		} else {
			var [auditLogs, eligibleRoles, userDetails, servicePrincipals, groups, roleDefinitions] = await Promise.all([
				retrieveAuditLogs(tenantId, requestedReport.fromDate),
				listEligibleRoles(tenantId),
				listUsers(tenantId, true),
				listServicePrincipals(tenantId, true),
				listGroups(tenantId, true),
				listEntraIdRoles(tenantId)
			])
		}

		if(eligibleRoles?.status == 400) {
			await reportModel.updatePIMReportStatus(requestedReport.id, 5) // Set the report status as PIM Licence missing
			return;
		}

		if(!eligibleRoles) {
			console.warn(`PIM Report ${requestedReport.id} failed due to no eligible roles`)
			await reportModel.updatePIMReportStatus(requestedReport.id, 4) // Set the report status as error
			return;
		}

		if(!userDetails) {
			console.warn(`PIM Report ${requestedReport.id} failed due to no userDetails`)
			await reportModel.updatePIMReportStatus(requestedReport.id, 4) // Set the report status as error
			return;
		}

		var allAccountDetails = userDetails;
		allAccountDetails = allAccountDetails.concat(servicePrincipals)

		// Loop through each identified eligible role
		let processedEligibleRoles = [];
		for(let i=0; i<eligibleRoles.length; i++) {
			console.log(`Report ${requestedReport.id} - Processing eligible role ${i+1} of ${eligibleRoles.length}`)

			let accountType = "User"
			let accountDetails = userDetails.find(user => user.id == eligibleRoles[i].principalId);
			if(!accountDetails) {
				accountType = "Service Principal"
				accountDetails = servicePrincipals.find(spn => spn.id == eligibleRoles[i].principalId);
			}
			if(!accountDetails) {
				accountType = "Group"
				accountDetails = groups.find(group => group.id == eligibleRoles[i].principalId);
			}

			if(accountType == "User" || accountType == "Service Principal") {
				var relatedAuditLogs = auditLogs.filter(log => log.id == accountDetails.id && log.TargetRoleId == eligibleRoles[i].roleDefinitionId)
				let roleDefinition = roleDefinitions.find(role => role.id == eligibleRoles[i].roleDefinitionId)
				let latestUsage = relatedAuditLogs?.length > 0 ? relatedAuditLogs.reduce((max, current) => {
					current = new Date(current.TimeGenerated)
					max = new Date(max.TimeGenerated)
					return current > max ? current : max;
				}) : null

				processedEligibleRoles.push({
					DisplayName: accountDetails.displayName,
					PrincipalName: accountDetails.userPrincipalName || accountDetails.appId,
					InUse: relatedAuditLogs.length > 0 ? 1 : 0,
					LastUsed: relatedAuditLogs.length > 0 ? convertKQLToSQLDatetime(latestUsage) : null,
					GroupName: "N/A",
					GroupDescription: "N/A",
					RoleDisplayName: roleDefinition.displayName,
					DirectoryScopeId: eligibleRoles[i].directoryScopeId,
					AccountType: accountType,
					AssignmentType: "Direct",
					AccountId: accountDetails.id,
					GroupId: null,
					RoleId: eligibleRoles[i].roleDefinitionId
				})
			} else if (accountType == 'Group') {
				for(let x=0; x<accountDetails.members.length; x++) {
					let member = allAccountDetails.find(account => account.id == accountDetails.members[x].id)
					var relatedAuditLogs = auditLogs.filter(log => log.id == member.id && log.TargetRoleId == eligibleRoles[i].roleDefinitionId)
					let roleDefinition = roleDefinitions.find(role => role.id == eligibleRoles[i].roleDefinitionId)
					let latestUsage = relatedAuditLogs?.length > 0 ? relatedAuditLogs.reduce((max, current) => {
						current = new Date(current.TimeGenerated)
						max = new Date(max.TimeGenerated)
						return current > max ? current : max;
					}) : null

					let memberType = member?.appId !== undefined ? 'ServicePrincipal' : 'User'

					processedEligibleRoles.push({
						DisplayName: member.displayName,
						PrincipalName: member.userPrincipalName || member.appId,
						InUse: relatedAuditLogs.length > 0 ? 1 : 0,
						LastUsed: relatedAuditLogs.length > 0 ? convertKQLToSQLDatetime(latestUsage?.TimeGenerated) : null,
						GroupName: accountDetails.displayName,
						GroupDescription: accountDetails.description,
						RoleDisplayName:roleDefinition.displayName,
						DirectoryScopeId: eligibleRoles[i].directoryScopeId,
						AccountType: memberType,
						AssignmentType: "Group",
						AccountId: member.id,
						GroupId: accountDetails.id,
						RoleId: eligibleRoles[i].roleDefinitionId
					})
				}
			}
		}

		// Save the role report
		console.log(`PIM Report ${requestedReport.id} - Saving results to Database`)
		const result = await reportModel.bulkSavePIMSuggestions(requestedReport.id, processedEligibleRoles)

		await axios.post(`https://rolesense.org/report/updateReportStatus?id=${requestedReport.id}&status=Ready`)
		await reportModel.updatePIMReportStatus(requestedReport.id, 3) // Set the report status as ready
		console.log(result);
	} catch (error) {
		console.error("Failed to process report");
		console.error(error);
		await axios.post(`https://rolesense.org/report/updateReportStatus?id=${requestedReport.id}&status=Failed`)
		await reportModel.updatePIMReportStatus(requestedReport.id, 4) // Set the report status as processing
		throw error
	}
}

function convertKQLToSQLDatetime(kqlDatetime) {
	// Parse the KQL datetime string into a JavaScript Date object
	const date = new Date(kqlDatetime);
  
	// Format the SQL datetime as 'YYYY-MM-DD HH:MM:SS'
	const year = date.getFullYear();
	const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are 0-based
	const day = String(date.getDate()).padStart(2, '0');
	const hours = String(date.getHours()).padStart(2, '0');
	const minutes = String(date.getMinutes()).padStart(2, '0');
	const seconds = String(date.getSeconds()).padStart(2, '0');
  
	return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }

// Retrieves all Activity Logs from Azure for the past 30 days
// TODO - store records in DB and only request records newer than the latest in DB
async function retrieveActivityLogs(tenantId, subscriptionId, resourceGroupName, fromDate) {
	const accessToken = await getAccessToken(tenantId, 'https://management.azure.com/.default');
	var startTime = new Date(fromDate)
	const formattedStartTime = getAzureFormattedTimestamp(startTime);

	const start = performance.now();

	var requestUrl = `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.Insights/eventtypes/management/values?api-version=2015-04-01&$select=authorization,caller,eventTimestamp&$filter=eventTimestamp ge '${formattedStartTime}'`
	if(resourceGroupName) {
		requestUrl += ` and resourceGroupName eq '${resourceGroupName}'`
	}

	let allRecords = [], response;

	try {
		while (requestUrl) {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data.nextLink || null;
		}

		const end = performance.now();
		console.log(`Time taken: ${end - start} ms`);

		return allRecords;

	} catch(error) {
		console.error('Error fetching details from Resource Manager Activity Logs API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching activity logs'
			}
	}
}


// Retrieves all Activity Logs from Azure for the past 30 days
// TODO - store records in DB and only request records newer than the latest in DB
async function retrieveAuditLogs(tenantId, fromDate) {
	const accessToken = await getAccessToken(tenantId, 'https://management.azure.com/.default');

	var startTime = new Date(fromDate)
	const formattedStartTime = getAzureFormattedTimestamp(startTime);

	var requestUrl = `https://management.azure.com/auditLogs/directoryAudits?$select=activityDateTime,TargetResources&$filter=activityDisplayName eq 'Add member to role completed (PIM activation)' and Result eq 'success' and activityDateTime gt ${formattedStartTime}`

	let allRecords = [], response;

	try {
		while (requestUrl) {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data.nextLink || null;
		}

		$returnedRecords = [];

		return allRecords;

	} catch(error) {
		console.error('Error fetching details from Resource Manager Audit Logs API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching audit logs'
			}
	}
}

async function retrieveAuditLogsFromWorkspace(tenantId, workspaceId, quantity, from) {
	var startTime = new Date(from)
	const formattedStartTime = getAzureFormattedTimestamp(startTime);

	const accessToken = await getAccessToken(tenantId, 'https://management.azure.com/.default');

	var url = `https://management.azure.com/${workspaceId}/query?api-version=2017-10-01`
	var body = {
		query: `AuditLogs 
				| where OperationName == 'Add member to role completed (PIM activation)' 
					and Result == 'success'
					and Category == 'RoleManagement'`
	}

	if(from && formattedStartTime) {
		body.query += `| where ActivityDateTime > todatetime('${formattedStartTime}')`
	}

	body.query += `| project TimeGenerated, Identity, UPN = TargetResources[2].userPrincipalName, id = TargetResources[2].id, TargetRole = TargetResources[0].displayName, TargetRoleId = TargetResources[0].id, Result`
	
	if(quantity) {
		body.query += `| top ${quantity} by TimeGenerated asc`
	}
	
	return await axios
		.post(url, body, {
			headers: { 
				Authorization: `Bearer ${accessToken}`
			}
		})
		.then(response => {
			// Handle successful response
			// Ensure there's data in the tables
			if (!response.data.tables || response.data.tables.length === 0) {
				console.error('No tables found in response.');
				return [];
			}
		
			// Get the first table (usually "PrimaryResult")
			const table = response.data.tables[0];
		
			// Extract columns and rows
			const columns = table.columns.map(col => col.name); // Array of column names
			const rows = table.rows; // Array of rows
		
			// Map rows to objects
			const result = rows.map(row => {
				const obj = {};
				row.forEach((value, index) => {
					obj[columns[index]] = value;
				});
				return obj;
			});
		
			return result;
		})
		.catch(error => {
			// Handle error
			console.error('Error fetching details from Log Analytics AuditLogs:', error.response.data);
			return []
		});
}

async function listRoleDefinitions(tenantId, subscriptionId, resourceGroupId) {
	const accessToken = await getAccessToken(tenantId, 'https://management.azure.com/.default');

	var requestUrl = `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.Authorization/roleDefinitions?&api-version=2022-04-01`
	if(resourceGroupId) {
		requestUrl = `https://management.azure.com/${resourceGroupId}/providers/Microsoft.Authorization/roleDefinitions?&api-version=2022-04-01`
	}

	// $filter=eventTimestamp ge '2014-07-16T04:36:37.6407898Z' and eventTimestamp le '2014-07-20T04:36:37.6407898Z' and resourceGroupName eq 'resourceGroupName

	return await axios
		.get(requestUrl, {
			headers: { 
				Authorization: `Bearer ${accessToken}`
			}
		})
		.then(response => {
			// Handle successful response
			return response.data.value;
		})
		.catch(error => {
			// Handle error
			console.error('Error fetching details from Resource Manager roleAssignments API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching user details'
			}
		});
}

async function listRoleAssignments(tenantId, subscriptionId, resourceGroupId) {
	const accessToken = await getAccessToken(tenantId, 'https://management.azure.com/.default');

	var requestUrl = `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01&$filter=atScope()`
	if(resourceGroupId) {
		requestUrl = `https://management.azure.com/${resourceGroupId}/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01&$filter=atScope()`
		// GET https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01
	}

	return await axios
		.get(requestUrl, {
			headers: { 
				Authorization: `Bearer ${accessToken}`
			}
		})
		.then(response => {
			// Handle successful response
			return response.data.value;
		})
		.catch(error => {
			// Handle error
			console.error('Error fetching details from Resource Manager roleAssignments API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching user details'
			}
		});
}

// Processes a given role Assignment and returns an object describing if it's required
function getRoleSuggestion(roleAssignment, roleDefinitions, requiredActions, userDetails) {
	let relatedRoleDefinition = roleDefinitions.find(definition => definition.id == roleAssignment.roleDefinitionId) // The specific role definition
	let roleProperties = relatedRoleDefinition.properties.permissions[0]

	// Determine what the role allows and disallows
	let allowedActions = [...roleProperties.actions, ...roleProperties.dataActions]
	let notActions = [...roleProperties.notActions, ...roleProperties.notDataActions]

	// Return which role actions are required
	let roleRequiredActions = requiredActions.filter(action => {
		// Check if at least some of the actions are required within the role
		const isAllowed = allowedActions.some(
			allowedAction => action === allowedAction || allowedAction === '*' || action.includes(allowedAction)
		)
		// Check if the role explicitly disallows the required action
		const isNotInNotActions = !notActions.includes(action);

		return isAllowed && isNotInNotActions;
	})

	// Dtermine if the role is required and if so what the minimum applicable role is
	let isRoleRequired = false, recommendedRoleName;
	if(roleRequiredActions.length > 0) {
		isRoleRequired = true
		recommendedRoleName = getLeastPrivilegedRole(roleRequiredActions, roleDefinitions)

	} else {
		recommendedRoleName = "Role can be removed"
	}

	let scopeType = getScopeType(roleAssignment)
	let relatedUser = userDetails.find(user => user.id == roleAssignment.principalId);

	return {
		userId: relatedUser?.id ?? roleAssignment.principalId,
		userName: relatedUser?.displayName ?? roleAssignment.principalId,
		userPrincipalName: relatedUser?.userPrincipalName ?? relatedUser?.appId,
		userType: roleAssignment.principalType,
		assignedRole: relatedRoleDefinition.properties.roleName,
		scope: roleAssignment.scope,
		scopeType: scopeType,
		roleRequired: isRoleRequired,
		requiredActions: roleRequiredActions.join(", "),
		recommendedRole: recommendedRoleName
	}
}

// Calculates the least-privileged role given the required array of actions and a list of Azure roles
function getLeastPrivilegedRole(requiredActions, allRoles) {
	const matchingRoles = [];

	allRoles.forEach((role) => {
		const permissions = role.properties.permissions[0];
		const allowedActions = [...permissions.actions, ...permissions.dataActions];
		const notActions = [...permissions.notActions, ...permissions.notDataActions];

		// Check if all required actions are included in allowed actions (considering wildcards)
		const missingActions = requiredActions.filter(action => 
			!allowedActions.some(allowedAction => 
				allowedAction === action || allowedAction === '*' || action.includes(allowedAction)
			)
		);

		// Exclude roles that have NotActions that intersect with required actions (considering wildcards)
		const conflictingNotActions = requiredActions.filter(action => 
			notActions.some(notAction => 
				notAction === action || notAction === '*' || action.includes(notAction)
			)
		);

		// Calculate role score
		if (missingActions.length === 0 && conflictingNotActions.length === 0) {
			let roleScore = 0;
			
			requiredActions.forEach(action => {
				const match = allowedActions.find(allowedAction => 
					allowedAction === action || allowedAction === '*' || action.includes(allowedAction)
				);

				if (match === action) {
					// Exact match
					roleScore += 1;
				} else if (match === '*') {
					// Match via '*'
					roleScore += 10;
				} else if (match && action.includes(match)) {
					// Match via wildcard
					roleScore += 5;
				}
			});

			// Store the role and its score
			matchingRoles.push({ role, score: roleScore });
		}
	});

	if (matchingRoles.length > 0) {
		// Sort the roles by score (lower score is better)
		// If scores are equal, prefer roles with fewer allowed actions
		matchingRoles.sort((a, b) => 
			a.score - b.score || a.role.properties.permissions[0].actions.length - b.role.properties.permissions[0].actions.length
		);

		// Select the role with the lowest score
		return matchingRoles[0].role.properties.roleName;
	}

	return null;;
}	

function getScopeType(assignment) {
	let scopeType = "undefined";
	const scopeChars = (assignment.scope.match(/\//g) || []).length;

	if (scopeChars === 1) {
		scopeType = "Global";
	} else if (scopeChars === 2) {
		scopeType = "Subscription";
	} else if (assignment.scope === "/providers/Microsoft.Management/managementGroups/Group") {
		scopeType = "Management Group";
	} else if (scopeChars === 4) {
		scopeType = "Resource Group";
	} else {
		scopeType = "Resource";
	}

	return scopeType;
}

// Get Entra ID Users
async function listUsers(tenantId, retrieveAll) {
	const accessToken = await getAccessToken(tenantId, 'https://graph.microsoft.com/.default');

	var requestUrl = `https://graph.microsoft.com/v1.0/users?$count`

	let allRecords = [], response;

	try {
		do {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`,
					consistencyLevel: 'eventual'
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data["@odata.nextLink"] || null;
		} while (requestUrl && retrieveAll)

		return allRecords;

	} catch(error) {
		console.error('Error fetching details from Resource Manager List Users API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching activity logs'
			}
	}
}

// Get Entra ID Service Principals
async function listServicePrincipals(tenantId, retrieveAll) {
	const accessToken = await getAccessToken(tenantId, 'https://graph.microsoft.com/.default');

	var requestUrl = `https://graph.microsoft.com/v1.0/servicePrincipals?$count`

	let allRecords = [], response;

	try {
		do {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`,
					consistencyLevel: 'eventual'
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data["@odata.nextLink"] || null;
		} while (requestUrl && retrieveAll)

		return allRecords;

	} catch(error) {
		console.error('Error fetching details from Resource Manager List Service Principals API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching activity logs'
			}
	}
}

async function listEntraIdRoles(tenantId) {
	const accessToken = await getAccessToken(tenantId, 'https://graph.microsoft.com/.default');

	var requestUrl = `https://graph.microsoft.com/v1.0/roleManagement/directory/roleDefinitions`

	let allRecords = [], response;

	try {
		do {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data["@odata.nextLink"] || null;
		} while (requestUrl && retrieveAll)

		return allRecords;

	} catch(error) {
		console.error('Error fetching details from Graph List Entra Roles API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching entraId roles'
			}
	}
}2135

async function listGroups(tenantId, retrieveAll) {
	const accessToken = await getAccessToken(tenantId, 'https://graph.microsoft.com/.default');

	var requestUrl = `https://graph.microsoft.com/v1.0/groups?$select=displayName,id,description,Members&$filter=isAssignableToRole eq true&$expand=members($select=id,displayName)`

	let allRecords = [], response;

	try {
		do {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`,
					consistencyLevel: 'eventual'
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data["@odata.nextLink"] || null;
		} while (requestUrl && retrieveAll)

		return allRecords;

	} catch(error) {
		console.error('Error fetching details from Resource Manager List Groups API:', error.response.data);
			return {
				status: 500,
				error: 'Error fetching Groups'
			}
	}
}

// https://learn.microsoft.com/en-us/graph/api/rbacapplication-list-roleeligibilityschedules?view=graph-rest-1.0&tabs=http
async function listEligibleRoles(tenantId) {
	const accessToken = await getAccessToken(tenantId, 'https://graph.microsoft.com/.default');

	var requestUrl = `https://graph.microsoft.com/v1.0/roleManagement/directory/roleEligibilitySchedules`

	let allRecords = [], response;

	try {
		do {
			response = await axios.get(requestUrl, {
				headers: { 
					Authorization: `Bearer ${accessToken}`,
					consistencyLevel: 'eventual'
				}
			})

			allRecords = allRecords.concat(response.data.value);
			requestUrl = response.data["@odata.nextLink"] || null;
		} while (requestUrl && retrieveAll)

		return allRecords;

	} catch(error) {
		if(error?.response?.data?.error?.message) {
			return {
				status: 400,
				error: error?.response?.data?.error?.message
			}
		}
		console.error('Error fetching details from Resource Manager listEligibleRoles API:', error.response.data);
		return {
			status: 500,
			error: 'Error fetching listEligibleRoles'
		}
	}
}

async function retrieveActivityLogs(tenantId, workspaceId, quantity, subscriptionId, fromDate) {
	subscriptionId = subscriptionId.toLowerCase();
	const accessToken = await getAccessToken(tenantId, 'https://management.azure.com/.default');

	var url = `https://management.azure.com/${workspaceId}/query?api-version=2017-10-01`
	var body = {
		query: `AzureActivity`
	}
	if(subscriptionId) {
		body.query += `| where SubscriptionId == '${subscriptionId}' and ActivityStatusValue == 'Success' and Caller contains '@' | project Caller, Authorization, TimeGenerated`
		if(quantity) {
			body.query += `| top ${quantity} by TimeGenerated asc`
		}
	}
	else {
		body.query += `| where ActivityStatusValue == 'Success' and Caller contains '@' | project Caller, Authorization, TimeGenerated`
		if(quantity) {
			body.query += `| top ${quantity} by TimeGenerated asc`
		}
	}

	if(fromDate) {
		var startTime = new Date(fromDate)
		const formattedStartTime = getAzureFormattedTimestamp(startTime);
		body.query += `| where TimeGenerated >= datetime(${formattedStartTime})`
	}
	
	return await axios
		.post(url, body, {
			headers: { 
				Authorization: `Bearer ${accessToken}`
			}
		})
		.then(response => {
			// Handle successful response
			// Ensure there's data in the tables
			if (!response.data.tables || response.data.tables.length === 0) {
				console.error('No tables found in response.');
				return [];
			}
		
			// Get the first table (usually "PrimaryResult")
			const table = response.data.tables[0];
		
			// Extract columns and rows
			const columns = table.columns.map(col => col.name); // Array of column names
			const rows = table.rows; // Array of rows
		
			// Map rows to objects
			const result = rows.map(row => {
				const obj = {};
				row.forEach((value, index) => {
					obj[columns[index]] = value;
				});
				return obj;
			});
		
			return result;
		})
		.catch(error => {
			// Handle error
			console.error('Error fetching details from Log Analytics:', error.response.data);
			return []
		});
}