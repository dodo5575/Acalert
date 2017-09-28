
$(function() {
    var submit_form = function(e) {
        $.getJSON($SCRIPT_ROOT + '/_query', {
            a: $('input[name="a"]').val(),
            b: $('input[name="b"]').val()
        }, function(data) {
            makePlotly(data.result);
            continuousQ();
        });
        return false;
    };
    $('button#calculate').bind('click', submit_form);
    $('input[type=text]').bind('keydown', function(e) {
        if (e.keyCode == 13) {
            submit_form(e);
        }
    });
});



function continuousQ() {
    setInterval(call, 1000);
};


function call() {
    $.getJSON($SCRIPT_ROOT + '/_query', {
        a: $('input[name="a"]').val(),
        b: $('input[name="b"]').val()
    }, function(data) {
        makePlotly(data.result);
    });
    return false;
};


var colors = ['6600CC',	'FFCC00', '000000', 'CC0000']


function makePlotly( allRows ){
    var x = [], y = [], mean = [], std = [];
    var xBox = [], yBox = [];
    var bound = [];

    // use regex to extract time information
    var time_pattern = new RegExp("[0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{6}", "m");

    for (var i=0; i<allRows.length; i++) {
        row = allRows[i];
        var reMatch = time_pattern.exec(row['time']);

        mean.push( row['mean'] );
        std.push( row['std'] );

        x.push( reMatch[0] );
        y.push( row['acc'] );

    }
    x = x.reverse();
    y = y.reverse();


    // find the boundary between each window
    bound.push(0);
    for (var i=0; i<x.length; i++) {
        if (i+1 < x.length && mean[i+1] != mean[i]) {
            bound.push(i);
            bound.push(i+1);
        }
    }
    bound.push(i-1);

    // get the DOM object for plotting
    var plotDiv = document.getElementById("chart1");
    var traces = [{
        x: x,
        y: y
    }];

    // Draw the window box
    shapes = [];
    for (var i=0; i<bound.length/2; i++) {
        shapes.push({
            type: 'rect',
            // x-reference is assigned to the x-values
            xref: 'x',
            // y-reference is assigned to the y-values
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

    // Compose layout
    var layout = {
  
        shapes,
        title: 'User ID '+$('input[name="a"]').val(),
        yaxis: {
            title: 'Acceleration'
        }

    }

    Plotly.newPlot('chart1', traces, layout);
};
