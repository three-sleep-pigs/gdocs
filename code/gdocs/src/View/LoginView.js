import React from "react";
import "../bootstrap-4.6.0-dist/bootstrap-4.6.0-dist/css/bootstrap.min.css";
import  "../CSS/LoginView.css";
import {withRouter} from "react-router-dom";
class LoginView extends React.Component{
    constructor(props) {
        super(props);
        this.onSubmit = this.onSubmit.bind(this);
    }
    onSubmit=(e)=> {
        e.preventDefault()
        let that=this
        console.log(document.getElementById('inputUsername').value);
        console.log(document.getElementById('inputPassword').value);
        let username=document.getElementById('inputUsername').value;
        let password=document.getElementById('inputPassword').value;
        fetch("http://localhost:8888/login",{
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
                console.log("status:"+res.status);
                // eslint-disable-next-line eqeqeq
                if(res.status===401){
                    console.log(res.error);
                    alert("密码错误");
                    return;
                }
                else if(res.status===402){
                    console.log(res.error);
                    alert("该用户名不存在");
                    return;
                }
                else if(res.status===200){
                    localStorage.username=username
                    that.props.history.push('menu')
                }
            }).catch(function (ex) {
            console.log('parsing failed', ex)
        })
    }

    render() {
        return(
            <div className="login-page">
                <form className="form-signin" onSubmit={this.onSubmit}>
                    <h1 className="h3 mb-3 font-weight-normal ">Please sign in</h1>
                    <label htmlFor="inputEmail" className="sr-only">Email address</label>
                    <input type="text" id="inputUsername" className="form-control" placeholder="Username" required
                           autoFocus/>
                    <label htmlFor="inputPassword" className="sr-only">Password</label>
                    <input type="password" id="inputPassword" className="form-control" placeholder="Password" required/>
                    <div className="checkbox mb-3">
                        <label>
                            <input type="checkbox" value="remember-me"/> Remember me
                        </label>
                    </div>
                    <button className="btn btn-lg btn-primary btn-block" type="submit">Sign in</button>
                </form>
            </div>
        );
    }
}

export default withRouter(LoginView);
