import React from "react";
import "../bootstrap-4.6.0-dist/bootstrap-4.6.0-dist/css/bootstrap.min.css";
import  "../CSS/RegisterView.css";
import {withRouter} from "react-router-dom";
class RegisterView extends React.Component{
    constructor(props) {
        super(props);
    }
    onSubmit=(e)=> {
        e.preventDefault()
        let that=this
        console.log(document.getElementById('inputUsername').value);
        console.log(document.getElementById('inputPassword').value);
        let username=document.getElementById('inputUsername').value;
        let password=document.getElementById('inputPassword').value;
        fetch("http://localhost:8888/register",{
            method:'POST',
            headers:{
                'Content-Type':'application/json;charset=UTF-8',
                'Access-Control-ALLow-Origin':"*"
            },
            body:JSON.stringify({
                username:username,
                passwords:password,
            }),
            mode:'cors',
            cache:"default"})
            .then(response => response.text())
            .then(data => {
                let res=JSON.parse(data);
                console.log("res"+res);
                // eslint-disable-next-line eqeqeq
                if(res==401){
                    console.log(res.error);
                    alert("该用户名已注册");
                    return;
                }
                else if(res==402){
                    console.log(res.error);
                    alert("出错了，请重试！");
                    return;
                }
                else if(res==200){
                    localStorage.username=username
                    that.props.history.push('/menu')
                }
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        })
    }
    render() {
        return(
            <div className="login-page">
                <form className="form-signin" onSubmit={this.onSubmit}>
                    <h1 className="h3 mb-3 font-weight-normal ">Please register</h1>
                    <label htmlFor="inputEmail" className="sr-only">Email address</label>
                    <input type="text" id="inputUsername" className="form-control" placeholder="Username" required
                           autoFocus/>
                    <label htmlFor="inputPassword" className="sr-only">Password</label>
                    <input type="password" id="inputPassword" className="form-control" placeholder="Password" required/>
                    <button className="btn btn-lg btn-primary btn-block" type="submit">Register</button>
                </form>
            </div>
        );
    }
}

export default RegisterView;
