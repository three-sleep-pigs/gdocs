import React from "react";
import "../bootstrap-4.6.0-dist/bootstrap-4.6.0-dist/css/bootstrap.min.css";
import  "../CSS/RegisterView.css";
class RegisterView extends React.Component{
    render() {
        return(
            <div className="login-page">
                <form className="form-signin">
                    <h1 className="h3 mb-3 font-weight-normal ">Please register</h1>
                    <label htmlFor="inputEmail" className="sr-only">Email address</label>
                    <input type="text" id="inputUsername" className="form-control" placeholder="Username" required
                           autoFocus/>
                    <label htmlFor="inputPassword" className="sr-only">Password</label>
                    <input type="password" id="inputPassword" className="form-control" placeholder="Password" required/>
                    <button className="btn btn-lg btn-primary btn-block" type="submit">Sign in</button>
                </form>
            </div>
        );
    }
}

export default RegisterView;
