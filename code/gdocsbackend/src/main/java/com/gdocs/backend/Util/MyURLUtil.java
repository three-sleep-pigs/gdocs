package com.gdocs.backend.Util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class MyURLUtil {
    /**
     * 解码
     * @param str
     * @return
     */
    public static String urlDecode(String str){
        try {
            String strReturn= URLDecoder.decode(str, "UTF-8");
            return strReturn;
        } catch (UnsupportedEncodingException e) {
            System.out.println("urlDecode error:"+str+" info:"+e.toString());
        }
        return null;
    }

    /**
     * 编码
     * @param str
     * @return
     */
    public static String urlEncode(String str){
        try {
            String strReturn= URLEncoder.encode(str, "UTF-8");
            return strReturn;
        } catch (UnsupportedEncodingException e) {
            System.out.println("urlEncode error:"+str+" info:"+e.toString());
        }
        return null;
    }

    //字符串转字节
    public static byte[] stringTobyte(String str){
        return stringTobyte(str,"ISO-8859-1");
    }
    public static byte[] stringTobyte(String str,String charsetName){
        try {
            return str.getBytes(charsetName);
        } catch (UnsupportedEncodingException e) {
            System.out.println("stringTobyte error:"+str+" info:"+e.toString());
        }
        return null;
    }

}
