/**
 * @author Volker Voss
 */
module rudp {
    exports net.rudp;
    exports net.rudp.impl;

    opens net.rudp;
    opens net.rudp.impl;
}