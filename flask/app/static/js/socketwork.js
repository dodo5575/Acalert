var socket = io('http://' + document.domain + ':' + location.port);

var maxNodesToProcess = 5000;
var displayedSize = 0;
var lastData;
var minDisplayedSizeForDemo = 25;

socket.on('connect', function() {
    resetZoom();
    console.log("connected");
});

socket.on('components', function(msg) {
    if (paused && displayedSize >= minDisplayedSizeForDemo)
	return;
    var newData = msg.data.new_val;
    console.log(newData);
    // Note: summing the counts via reduce may be more efficient
    var newDataSize = _.size(newData.clusters);
    
    if (newDataSize > maxNodesToProcess) {
	console.log("Nope! too much to do", newDataSize);
	return;
    }
    
    displayedSize = newDataSize;
    lastData = newData;
    
    updatePulseGraph(newData.clusters);
    if (!isMobile.any) {
	$('#barChart').show();
	$('#mobile-optimized-barchart').hide();
	updatePlot(newData.counts);
    }
});


socket.on('error', function(err) {
    console.log("error returned: ", err);
});
