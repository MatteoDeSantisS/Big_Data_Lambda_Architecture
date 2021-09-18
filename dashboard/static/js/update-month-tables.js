const UPDATE_MTIME = 200 //seconds to update month tables

//Update NO2 Table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-month-no2",
        dataType: "json",
    })
    .done(function(data){
        console.log(data);
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Mese </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> No2 Aqi </th> \
                            <th scope="col"> No2 Media </th> \
                        </tr>';
        var anchor = document.getElementById("no2-table")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'no2-table',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_MTIME);

//Update SO2 Table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-month-so2",
        dataType: "json",
    })
    .done(function(data){
        console.log(data);
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Mese </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> So2 Aqi </th> \
                            <th scope="col"> So2 Media </th> \
                        </tr>';
        var anchor = document.getElementById("so2-table")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'so2-table',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_MTIME);

//Update CO table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-month-co",
        dataType: "json",
    })
    .done(function(data){
        console.log(data);
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Mese </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> Co Aqi </th> \
                            <th scope="col"> Co Media </th> \
                        </tr>';
        var anchor = document.getElementById("co-table")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'co-table',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_MTIME);

//Update O3 table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data-month-o3",
        dataType: "json",
    })
    .done(function(data){
        console.log(data);
        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Mese </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> O3 Aqi </th> \
                            <th scope="col"> O3 Media </th> \
                        </tr>';
        var anchor = document.getElementById("o3-table")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'o3-table',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_MTIME);