package gr.forth.ics.virtuoso;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author rousakis
 */
public class TripleString {

    private final String triple;

    public TripleString(String s, String p, String o, Triple_Type type) {
        if (type == Triple_Type.LITERAL) {
            triple = "<" + s + "> <" + p + "> \"" + o + "\"";
        } else {
            triple = "<" + s + "> <" + p + "> <" + o + ">";
        }
    }

    public String getTripleString() {
        return triple;
    }
}
