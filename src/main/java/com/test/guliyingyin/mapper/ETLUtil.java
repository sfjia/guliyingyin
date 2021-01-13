package com.test.guliyingyin.mapper;

import org.apache.commons.lang.StringUtils;

/**
 * @Author 贾
 * @Date 2020/11/89:37
 *
 *
 * 1.过滤脏数据
 * 2.去掉类别中的空格
 * 3.关联视频中的分割
 *
 */
public class ETLUtil {

    public static String etlStr(String line){
        if(StringUtils.isBlank(line)) {
            return null;
        }
        String[] split = line.split("\t");
        int length = split.length;
        if(length < 9 ){
            return null;
        }
        split[3] = split[3].replaceAll(" ", "");
        
        //
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i <split.length ; i++) {
            if(i<=9 && i!=split.length-1){
                sb.append(split[i]).append("\t");
            }else if(i>9 && i!= split.length-1){
                sb.append(split[i]).append("&");
            }else {
                sb.append(split[i]);
            }

        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String s = "LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34\t1305\t744";
        String str = etlStr(s);
        System.out.println("str = " + str);
    }
}
