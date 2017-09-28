//function makeplot() {
//  Plotly.d3.csv("static/total_visits.csv", function(data){ processData(data) } );
//
//};


$(function() {
  var submit_form = function(e) {
    $.getJSON($SCRIPT_ROOT + '/_add_numbers', {
      a: $('input[name="a"]').val(),
      b: $('input[name="b"]').val()
    }, function(data) {
      //$('#result').text(data.result);
      makePlotly(data.result);
      $('input[name=a]').focus().select();
      continuousQ();
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



function continuousQ() {
 setInterval(call, 1000);
};

function call() {
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


var colors = ['6600CC',	'FFCC00', '000000', 'CC0000']


function makePlotly( allRows ){
  console.log(allRows);
  var x = [], y = [], mean = [], std = [];
  var xBox = [], yBox = [];
  var bound = [];

  var time_pattern = new RegExp("[0-9]{2}:[0-9]{2}:[0-9]{2}", "m");

  for (var i=0; i<allRows.length; i++) {
    row = allRows[i];
    var reMatch = time_pattern.exec(row['time']);
    console.log(row);

    mean.push( row['mean'] );
    std.push( row['std'] );

    x.push( reMatch[0] );
    y.push( row['acc'] );
  }
  x = x.reverse();
  y = y.reverse();

  var uniqueMean = [...new Set(mean)];
  console.log(mean);
  console.log(uniqueMean);

  bound.push(0);
  for (var i=0; i<x.length; i++) {
    if (i+1 < x.length && mean[i+1] != mean[i]) {
      bound.push(i);
      bound.push(i+1);
    }
  }
  bound.push(i-1);


  console.log( 'X',x, 'Y',y);
  console.log( 'mean',mean, 'std',std, 'bound',bound);
  var plotDiv = document.getElementById("chart1");
  var traces = [{
    x: x,
    y: y
  }];

  shapes = [];
  for (var i=0; i<bound.length/2; i++) {
     shapes.push({
              type: 'rect',
              // x-reference is assigned to the x-values
              xref: 'x',
              // y-reference is assigned to the plot paper [0,1]
              yref: 'y',
              x0: x[bound[2*i]],
              y0: mean[bound[2*i]] - 3 * std[bound[2*i]],
              x1: x[bound[2*i+1]],
              y1: mean[bound[2*i]] + 3 * std[bound[2*i]],
              fillcolor: colors[i%4],
              opacity: 0.2,
              line: {
                  width: 0
              }
          });
  }


  var layout = {
  
      // to highlight the timestamp we use shapes and create a rectangular
  
      //shapes: [
      //    // 1st highlight during Feb 4 - Feb 6
      //    {
      //        type: 'rect',
      //        // x-reference is assigned to the x-values
      //        xref: 'x',
      //        // y-reference is assigned to the plot paper [0,1]
      //        yref: 'y',
      //        x0: '2015-02-04',
      //        y0: 0,
      //        x1: '2015-02-06',
      //        y1: 1,
      //        fillcolor: '#d3d3d3',
      //        opacity: 0.2,
      //        line: {
      //            width: 0
      //        }
      //    },
  
      //]
      shapes,
      title: 'User ID '+$('input[name="a"]').val()
  }


  Plotly.newPlot('chart1', traces,
    layout);
};
//  makeplot();
