package edu.caltech.nanodb.util;

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/18/11
 * Time: 7:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class RegexFileFilter implements FileFilter {

    private Pattern pattern;


    private Matcher matcher;


    public RegexFileFilter(String regex) {
        pattern = Pattern.compile(regex);
        matcher = pattern.matcher("");
    }


    @Override
    public boolean accept(File file) {
        matcher.reset(file.getName());
        return matcher.matches();
    }
}
