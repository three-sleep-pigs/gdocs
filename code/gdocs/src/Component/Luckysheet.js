import React from 'react';
class Luckysheet extends React.Component {
    constructor(props) {
        super(props);
        console.log(this.props.id)
        console.log(this.props.filename)
    }
    componentDidMount() {
        const luckysheet = window.luckysheet;
        var socket_url = "ws://" + window.location.host + "/excelSocket/"+localStorage.username+"/"+this.props.id;
        luckysheet.create({
            container: "luckysheet",
            plugins:['chart'],
            title:this.props.filename,
            lang:'zh',
            allowEdit: true,
            allowUpdate:true,
            myFolderUrl: "/menu",
            userInfo: '<i style="font-size:16px;color:#ff6a00;" class="fa fa-user-circle" aria-hidden="true">[[${userName}]]</i>',
            loadUrl: "/publicApi/excel/downData?id="+this.props.id,
            updateUrl: socket_url,
            // hook:{
            //     updated:function(e){
            //         var autoSave
            //         //监听更新,并在3s后自动保存
            //         if(autoSave) clearTimeout(autoSave)
            //         autoSave = setTimeout(function(){
            //             var excel = luckysheet.getAllSheets();
            //             console.log(excel[0].celldata)
            //         },3 * 1000)
            //         return true;
            //     }
            // },
        });
    }
    render() {
        const luckyCss = {
            margin: '0px',
            padding: '0px',
            position: 'absolute',
            width: '100%',
            height: '100%',
            left: '0px',
            top: '0px'
        }
        return (
            <div
    id="luckysheet"
    style={luckyCss}
    />
        )
    }
}

export default Luckysheet