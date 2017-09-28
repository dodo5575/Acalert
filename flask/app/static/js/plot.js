//function makeplot() {
//  Plotly.d3.csv("static/total_visits.csv", function(data){ processData(data) } );
//
//};

//function processData(allRows) {
//
//  makePlotly( x, y, standard_deviation );
//}

$(function() {
  var submit_form = function(e) {
    $.getJSON($SCRIPT_ROOT + '/_add_numbers', {
      a: $('input[name="a"]').val(),
      b: $('input[name="b"]').val()
    }, function(data) {
      //$('#result').text(data.result);
      makePlotly(data.result);
      $('input[name=a]').focus().select();
    });
    return false;
  };
  $('a#calculate').bind('click', submit_form);
  $('input[type=text]').bind('keydown', function(e) {
    if (e.keyCode == 13) {
      submit_form(e);
    }
  });
  $('input[name=a]').focus();
});


function makePlotly( allRows ){
  console.log(allRows);
  var x = [], y = [], standard_deviation = [];

  var time_pattern = new RegExp("[0-9]{2}:[0-9]{2}:[0-9]{2}", "m");

  for (var i=0; i<allRows.length; i++) {
    row = allRows[i];
    var reMatch = time_pattern.exec(row['time']);
    console.log(row['time']);

    x.push( reMatch[0] );
    y.push( row['acc'] );
  }
  x = x.reverse();
  y = y.reverse();

  console.log( 'X',x, 'Y',y, 'SD',standard_deviation );
  var plotDiv = document.getElementById("chart1");
  var traces = [{
    x: x,
    y: y
  }];

  Plotly.newPlot('chart1', traces,
    {title: 'User ID '+$('input[name="a"]').val()});
};
//  makeplot();
