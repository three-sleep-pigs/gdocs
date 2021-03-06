import React from 'react';
class Luckysheet extends React.Component {
    constructor(props) {
        super(props);
        console.log(this.props.id)
        console.log(this.props.filename)
        console.log(this.props.version)
        console.log(this.props.edit)
        console.log(localStorage.getItem("username"))
    }
    componentDidMount() {
        const luckysheet = window.luckysheet;
        var socket_url = "ws://127.0.0.1:8888/excelSocket/"+localStorage.getItem('username')+"/"+this.props.id+"/"+this.props.version;
        luckysheet.create({
            container: "luckysheet",
            plugins:['chart'],
            title:this.props.filename,
            lang:'zh',
            allowEdit: true,
            allowUpdate:true,
            myFolderUrl: "/menu",
            userInfo: '<i style="font-size:16px;color:#ff6a00;" class="fa fa-user-circle" aria-hidden="true">' + localStorage.getItem('username') + '</i>',
            loadUrl: "http://localhost:8888/publicApi/excel/downData?id="+this.props.id+"&version="+this.props.version+"&edit="+this.props.edit,
            updateUrl: socket_url,
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
