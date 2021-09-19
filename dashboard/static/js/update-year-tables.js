const UPDATE_YTIME = 600 //seconds to update year tables

//Update NO2 Table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-year-no2",
        dataType: "json",
    })
    .done(function(data){
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> NO2 Aqi </th> \
                            <th scope="col"> NO2 Media </th> \
                        </tr>';
        var anchor = document.getElementById("no2-ytable")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'yno2-table',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_YTIME);

//Update SO2 Table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-year-so2",
        dataType: "json",
    })
    .done(function(data){
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> SO2 Aqi </th> \
                            <th scope="col"> SO2 Media </th> \
                        </tr>';
        var anchor = document.getElementById("so2-ytable")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'so2-ytable',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_YTIME);

//Update CO table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-year-co",
        dataType: "json",
    })
    .done(function(data){
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> CO Aqi </th> \
                            <th scope="col"> CO Media </th> \
                        </tr>';
        var anchor = document.getElementById("co-ytable")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'co-ytable',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_YTIME);

//Update O3 table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-year-o3",
        dataType: "json",
    })
    .done(function(data){
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> O3 Aqi </th> \
                            <th scope="col"> O3 Media </th> \
                        </tr>';
        var anchor = document.getElementById("o3-ytable")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'o3-ytable',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_YTIME);