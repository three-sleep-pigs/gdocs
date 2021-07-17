import React from "react";
import '../bootstrap-4.6.0-dist/bootstrap-4.6.0-dist/css/bootstrap.min.css'
import '../CSS/MenuView.css'
import {Link} from "react-router-dom";
class HistoryView extends React.Component{
    constructor(props) {
        super(props);
        this.state={
            records:[],
        };
        console.log(localStorage.username)
        console.log(this.props.location.state.id)
        this.getHistory();
    }
    rollback=(id)=>{
        let file=this.props.location.state.id
        fetch("http://localhost:8888/rollback",{
            method:'POST',
            headers:{
            'Content-Type':'application/json;charset=UTF-8',
            'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                file:file,
                edit:id,
                username:localStorage.getItem("username")
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                if(data===200){
                    alert("回滚成功")
                    this.getHistory()
                }
                else{
                    alert("回滚失败");
                }
        }).catch(function (ex) {
            console.log('parsing failed', ex)
        });
    }
    getHistory(){
        let that=this;
        fetch("http://localhost:8888/getEditRecord",{
            method:'POST',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                id:this.props.location.state.id,
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.json())
            .then(data => {
                console.log(data);
                data.map(item=>{
                    if(item.operation===0) item.operation="创建";
                    else if(item.operation===1) item.operation="修改";
                    else if(item.operation===2) item.operation="删除";
                    else if(item.operation===3) item.operation="恢复";
                    else if(item.operation===4) item.operation="回滚";
                    return item;
                });
                that.setState({
                    records: data,
                });
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        })
    }

    render() {
        return(
            <div className="menu-page">
                <main role="main" className="container">
                    <div className="d-flex align-items-center p-3 my-3 text-white-50 bg-purple rounded shadow-sm">
                        <div className="lh-100">
                            <h6 className="mb-0 text-white lh-100">{this.props.filename}编辑记录</h6>
                            <small>excel</small>
                        </div>
                    </div>

                    <div className="my-3 p-3 bg-white rounded shadow-sm">
                        <div className="media text-muted pt-3">
                            <div className="row mb-4 media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
                                <h6 className="col-md-3 themed-grid-col">编辑人</h6>
                                <h6 className="col-md-3 themed-grid-col">编辑时间</h6>
                                <h6 className="col-md-3 themed-grid-col">编辑类型</h6>
                                <h6 className="col-md-3 themed-grid-col">操作</h6>
                            </div>
                        </div>
                        {
                             (this.state.records.map(item=>(
                                    <div className="media text-muted pt-3" key={item.id}>
                                        <div className="row mb-4 media-body pb-3 mb-0 small lh-125 border-bottom border-gray">
                                            <div className="col-md-3 themed-grid-col">{item.editor}</div>
                                            <div className="col-md-3 themed-grid-col">{item.edittime}</div>
                                            <div className="col-md-3 themed-grid-col">{item.operation}</div>
                                            <div className="col-md-3 themed-grid-col">
                                                <Link to={{pathname:"/excel",state:{id:item.fileid,filename:"",version:item.version,edit:item.id}}} className="col-md-4 themed-grid-col">
                                                    <button type="button" className="btn btn-info btn-sm" >查看</button>
                                                </Link>
                                                <button type="button" className="btn btn-link btn-sm" onClick={()=>this.rollback(item.id)}>回滚</button>
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

export default HistoryView;
