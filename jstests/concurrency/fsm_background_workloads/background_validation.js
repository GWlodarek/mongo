'use strict';

/**
 * background_validation.js
 *
 * Runs background validation against a standalone server.
 *
 */

load('jstests/concurrency/fsm_libs/extend_workload.js');                  // for extendWorkload
load('jstests/concurrency/fsm_background_workloads/background_base.js');  // for $config

var $config = extendWorkload($config, function($config, $super) {
    $config.teardown = function(db, collName, cluster) {

        var res = db[collName].validate({full: false, background: true});

        if (res.ok) {
            assertAlways.eq(true, res.valid, tojson(res));
        }

        $super.teardown.apply(this, arguments);

    };

    return $config;
});
