const UPDATE_TIME = 10 //seconds to update tables

//Update No2 Table
setInterval(function() {
    $.ajax({
        type: "POST",
        url: "/data",
        dataType: "json",
    })
    .done(function(data){
        console.log(data);
        var tableHtml = '<tr> \
                            <th scope="col"> Year </th> \
                            <th scope="col"> Month </th> \
                            <th scope="col"> State </th> \
                            <th scope="col"> City </th> \
                            <th scope="col"> No2 Mean </th> \
                            <th scope="col"> No2 Aqi </th> \
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
}, 1000 * UPDATE_TIME);