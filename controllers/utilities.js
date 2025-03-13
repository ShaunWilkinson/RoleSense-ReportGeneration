function getAzureFormattedTimestamp(now) {
    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, '0');
    const day = String(now.getUTCDate()).padStart(2, '0');
    const hours = String(now.getUTCHours()).padStart(2, '0');
    const minutes = String(now.getUTCMinutes()).padStart(2, '0');
    const seconds = String(now.getUTCSeconds()).padStart(2, '0');
    const milliseconds = String(now.getUTCMilliseconds()).padStart(3, '0');

    // Generate a 7-digit microsecond-like value (just extending milliseconds)
    const extendedMilliseconds = `${milliseconds}0000`.slice(0, 7);

    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${extendedMilliseconds}Z`;
}

module.exports = {getAzureFormattedTimestamp}