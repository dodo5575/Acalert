var socket = io('http://' + document.domain + ':' + location.port);

socket.on('connect', function() {
    console.log("connected");
});

socket.on('components', function(msg) {
    var newData = msg.data.new_val;
    //console.log(newData);
    processData(newData);
});

socket.on('error', function(err) {
    console.log("error returned: ", err);
});

function processData(allRows) {

  if (parseInt(allRows['userid']) < 5) {
    //console.log(allRows);
    
    var label = 'userid_'+allRows['userid'];

    document.getElementById(label).innerHTML = allRows['status'];

    if (allRows['status'] == 'safe') {
      document.getElementById(label).style.backgroundColor = '#82E0AA';
    } else {
      document.getElementById(label).style.backgroundColor = '#E74C3C';
    }
  }
  //var x = [], y = [], standard_deviation = [];

  //for (var i=0; i<allRows.length; i++) {
  //  row = allRows[i];
  //  x.push( i );
  //  y.push( row['acc'] );
  //}
  //console.log( 'X',x, 'Y',y, 'SD',standard_deviation );
  //makePlotly( x, y, standard_deviation );
};

function makePlotly( x, y, standard_deviation ){
  var plotDiv = document.getElementById("plot");
  var traces = [{
    x: x,
    y: y
  }];

  Plotly.newPlot('chart1', traces,
    {title: 'Plotting CSV data from AJAX call'});
};
