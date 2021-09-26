const UPDATE_DTIME = 10 //seconds to update month tables

//Update daily table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/daily-data",
        dataType: "json",
    })
    .done(function(data){

        var tableHtml = '<tr> \
                            <th scope="col"> Anno </th> \
                            <th scope="col"> Mese </th> \
                            <th scope="col"> Giorno </th> \
                            <th scope="col"> Stato </th> \
                            <th scope="col"> Contea </th> \
                            <th scope="col"> Città </th> \
                            <th scope="col"> CO Aqi </th> \
                            <th scope="col"> NO2 Aqi </th> \
                            <th scope="col"> O3 Aqi </th> \
                            <th scope="col"> SO2 Aqi </th> \
                        </tr>';
        var anchor = document.getElementById("daily-table")

        for (row of data){
            tableHtml += '<tr>';
                for(cell of row)
                    tableHtml += '<td>' + cell + '</td>';
            tableHtml += '</tr>';
        }

        var updatedTable = document.createElement('table')
        Object.assign(updatedTable, {
            id: 'daily-table',
            className: "table thead-light table-dark table-striped"
        })
        updatedTable.innerHTML = tableHtml
        
        anchor.parentNode.replaceChild(updatedTable, anchor)

    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.log(jqXHR, textStatus, errorThrown);
    });
}, 1000 * UPDATE_DTIME);
