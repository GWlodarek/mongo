(function() {

    "use strict";

    const t = db.jstests_background_validation;
    t.drop();

    const s1 = startParallelShell(function() {
        const t = db.jstests_background_validation;
        Random.setRandomSeed();

        function randomStr() {
            var result = "";
            var chars = "abcdefghijklmnopqrstuvwxyz";

            for (var i = 0; i < 25; i++) {
                result += chars.charAt(Math.floor(Random.rand() * chars.length));
            }

            return result;
        }

        for (var i = 0; i < 100; ++i) {
            t.drop();
            var res = t.ensureIndex({x: -1});
            assert.eq(1, res.ok, tojson(res));

            var res = t.ensureIndex({y: 1});
            assert.eq(1, res.ok, tojson(res));

            var res = t.ensureIndex({z: 1});
            assert.eq(1, res.ok, tojson(res));

            for (var j = 0; j < 50; ++j) {
                // Insert a random document every iteration
                t.save({x: j, y: randomStr(), z: [j, j * 3, j % 3, randomStr()]});

                // Every 5 iterations remove a random document
                if (j % 5 == 0) {
                    t.remove({x: Math.floor(Math.random() * j) + 1});
                }

                // Every 3 iterations update a random document
                if (j % 3 == 0) {
                    t.update(
                        {x: Math.floor(Math.random() * j) + 1},
                        {$set: {y: randomStr(), z: [randomStr(), randomStr(), j, randomStr()]}});
                }
            }
        }
    });

    const s2 = startParallelShell(function() {
        const t = db.jstests_background_validation;
        for (var i = 0; i < 800; ++i) {
            var res = t.validate({full: false, background: true});

            if (res.ok) {
                assert.eq(true, res.valid);
            }
            sleep(50);
        }

    });

    s1();
    s2();

})();
