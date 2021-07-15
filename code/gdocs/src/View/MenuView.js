import React from "react";
import '../bootstrap-4.6.0-dist/bootstrap-4.6.0-dist/css/bootstrap.min.css'
import '../CSS/MenuView.css'
import {Link} from "react-router-dom";
class MenuView extends React.Component{
    constructor(props) {
        super(props);
        this.state={
            ifBin:false,
            files:[],
            bin:[],
            pageName:"文档列表"
        };
        console.log(localStorage.username)
        this.getFiles();
        this.getBin();
        this.deleteFile=this.deleteFile.bind(this);
        this.createFile=this.createFile.bind(this);
    }
    getFiles(){
        let that=this;
        fetch("http://localhost:8888/getFiles",{
            method:'GET',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                console.log(data);
                that.setState({
                    files: data,
                });
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        })
    }
    getBin(){
        let that=this;
        let username=localStorage.username;
        fetch("http://localhost:8888/getBin",{
            method:'POST',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                username:username,
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                console.log(data);
                that.setState({
                    bin: data,
                });
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        })
    }
    deleteFile=(id)=>{
        console.log("click",id);
        let that=this;
        let username=localStorage.username;
        fetch("http://localhost:8888/deleteFile",{
            method:'POST',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                username:username,
                id:id,
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                if(data===200){
                    this.getFiles()
                    this.getBin()
                }
                else if(data===401){
                    alert("删除失败");
                }
                else if(data===402){
                    alert("该文件不存在")
                }
                else if(data===403){
                    alert("您没有权限删除该文件");
                }
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        });
    }

    recoverFile=(id)=>{
        console.log("click",id);
        let that=this;
        let username=localStorage.username;
        fetch("http://localhost:8888/recoverFile",{
            method:'POST',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                username:username,
                id:id,
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                if(data===200){
                    this.getFiles()
                    this.getBin()
                }
                else if(data===400){
                    alert("恢复文件失败");
                }
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        })
    }

    createFile=()=>{
        let that=this;
        let filename=document.getElementById('inputFilename').value;
        let username=localStorage.username;
        fetch("http://localhost:8888/addFile",{
            method:'POST',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                username:username,
                filename:filename,
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                console.log(data.status)
                if(data.status===200){
                    that.setState(
                        {
                            files:that.state.files.push(data.gfile)
                        }
                    )
                }
                else {
                    alert("新建文件失败");
                }
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        });
    }

    gotoFile=()=>{
        this.setState({
            ifBin:false,
            pageName:"文档列表"
        });
    };

    gotoBin=()=>{
        this.setState({
            ifBin:true,
            pageName:"回收站"
        });
    }
    render() {
        return(
            <div className="menu-page">
                <div className="nav-scroller bg-white shadow-sm">
                    <nav className="nav nav-underline">
                        <a className="nav-link active" onClick={()=>this.gotoFile()}>文档列表</a>
                        <a className="nav-link" onClick={()=>this.gotoBin()}>回收站</a>
                        <form className="form-inline my-2 my-lg-0 ">
                            <input type="text" id="inputFilename" className="form-control" placeholder="new filename" />
                            <button className="btn btn-outline-success my-2 my-sm-0" onClick={()=>this.createFile()}>新建文档</button>
                        </form>
                    </nav>
                </div>

                <main role="main" className="container">
                    <div className="d-flex align-items-center p-3 my-3 text-white-50 bg-purple rounded shadow-sm">
                        <div className="lh-100">
                            <h6 className="mb-0 text-white lh-100">{this.state.pageName}</h6>
                            <small>excel</small>
                        </div>
                    </div>

                    <div className="my-3 p-3 bg-white rounded shadow-sm">
                        <div className="media text-muted pt-3">
                            <div className="row mb-4 media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
                                <h6 className="col-md-4 themed-grid-col ">文件名</h6>
                                <h6 className="col-md-3 themed-grid-col">来自</h6>
                                <h6 className="col-md-2 themed-grid-col">最近修改</h6>
                                <h6 className="col-md-3 themed-grid-col">操作</h6>
                            </div>
                        </div>
                        {
                            this.state.ifBin? (this.state.bin.map(item=>(
                                <div className="media text-muted pt-3" key={item.id}>
                                    <div className="row mb-4 media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
                                        <Link to={{pathname:"/excel",state:{id:item.id,filename:item.filename}}} className="col-md-4 themed-grid-col">{item.filename}</Link>
                                        <div className="col-md-3 themed-grid-col">{item.creator}</div>
                                        <div className="col-md-2 themed-grid-col">{item.recent}</div>
                                        <div className="col-md-3 themed-grid-col">
                                            <button type="button" className="btn btn-danger btn-sm" onClick={()=>this.recoverFile(item.id)}>恢复</button>
                                        </div>
                                    </div>
                                </div>
                                )
                            )):
                                (this.state.files.map(item=>(
                            <div className="media text-muted pt-3" key={item.id}>
                                <div className="row mb-4 media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
                                    <Link to={{pathname:"/excel",state:{id:item.id,filename:item.filename}}} className="col-md-4 themed-grid-col">{item.filename}</Link>
                                    <div className="col-md-3 themed-grid-col">{item.creator}</div>
                                    <div className="col-md-2 themed-grid-col">{item.recent}</div>
                                    <div className="col-md-3 themed-grid-col">
                                        <button type="button" className="btn btn-danger btn-sm" onClick={()=>this.deleteFile(item.id)}>删除</button>
                                    </div>
                                </div>
                            </div>
                                )
                            ))
                        }
                    </div>

                </main>
            </div>
        );
    }
}

export default MenuView;
