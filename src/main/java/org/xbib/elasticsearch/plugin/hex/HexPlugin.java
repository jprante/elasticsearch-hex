
package org.xbib.elasticsearch.plugin.hex;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.xbib.elasticsearch.action.bulk.BulkAction;
import org.xbib.elasticsearch.action.bulk.TransportBulkAction;
import org.xbib.elasticsearch.rest.hex.action.hex.RestBulkHexAction;

public class HexPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "hex";
    }

    @Override
    public String description() {
        return "Hex plugin";
    }

    public void onModule(ActionModule module) {
        module.registerAction(BulkAction.INSTANCE, TransportBulkAction.class);
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestBulkHexAction.class);
    }

}
