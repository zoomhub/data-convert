var fs = require('fs');
var _ = require('underscore');
var readline = require('readline');
var stream = require('stream');


var LineReader = function(config) {
  console.log('processing ' + config.name);
  var instream = fs.createReadStream(config.name);
  var outstream = new stream();
  var linesToProcess = [];
  var processing = false;
  var done = false;
  var rl = readline.createInterface(instream, outstream);

  var processNext = function() {
    if (!linesToProcess.length) {
      processing = false;
      if (done) {
        config.processDone();
      }

      return;
    }

    var line = linesToProcess.shift();
    config.processLine(line, processNext);
  };

  rl.on('line', function(line) {
    linesToProcess.push(line);
    if (!processing) {
      processing = true;
      processNext();
    }
  });

  rl.on('close', function() {
    console.log('done with ' + config.name);
    done = true;
    if (!processing) {
      config.processDone();
    }
  });
};

// ----------
var processAnalytics = function() {
  var output = {};

  new LineReader({
    name: 'input/analytics.tsv',
    processLine: function(line, next) {
      var parts = line.split('\t');
      if (parts.length <= 1) {
        next();
        return;
      }

      var path = parts[0];
      var id = path.replace(/^\/([^.]*).*$/, '$1');
      if (id === 'Page path level 1') {
        next();
        return;
      }

      var isEmbed = /^\/[^.]*\.js/.test(path);
      if (!output[id]) {
        output[id] = {
          id: id,
          page: 0,
          embed: 0,
          total: 0
        };
      }

      var count = parseInt(parts[1], 10);
      if (isEmbed) {
        output[id].embed += count;
      } else {
        output[id].page += count;
      }

      output[id].total += count;

      // console.log(id);
      next();
    },
    processDone: function() {
      var writeOut = function(key) {
        var text = '';
        _.chain(output)
          .map(function(v, k) {
            return v;
          })
          .filter(function(v, i) {
            return !!v[key];
          })
          .sortBy(function(v, i) {
            return -v[key];
          })
          .each(function(v, i) {
            text += v.id + ': ' + v[key] + '\n';
          });

        fs.writeFile('output2/' + key + '.txt', text, function(err) {
          if (err) {
            console.log('error writing file for ' + key, err);
          }

          console.log('success: ' + key);
        });
      };

      writeOut('embed');
      writeOut('page');
      writeOut('total');
    }
  });
};

// ----------
var processContentInfo = function() {
  new LineReader({
    name: 'input/ContentInfo.txt',
    processLine: function(line, next) {
      var parts = line.split('\t');
      if (parts.length <= 1) {
        next();
        return;
      }

      var id = parts[8];
      if (!id) {
        console.log('bad ID: ' + line);
        next();
        return;
      }

      if (id === 'Id') {
        next();
        return;
      }

      id = id.replace(/([A-Z])/g, '_$1');

      var info = {
        attributionLink: parts[4],
        attributionText: parts[5],
        mime: parts[9],
        size: parseInt(parts[12], 10),
        title: parts[14],
        url: parts[16]
      };

      if (info.size <= 0) {
        console.log('zero size file: ' + id);
        next();
        return;
      }

      if (!info.attributionLink) {
        delete info.attributionLink;
      }

      if (!info.attributionText) {
        delete info.attributionText;
      }

      if (!info.title) {
        delete info.title;
      }

      // console.log(JSON.stringify(info, null, 2));
      var fileName = 'output/' + id + '.json';
      fs.exists(fileName, function(exists) {
        if (exists) {
          console.log('skipping ' + id + ' because it already exists');
          next();
          return;
        }

        fs.writeFile(fileName, JSON.stringify(info, null, 2), function(err) {
          if (err) {
            console.log('error writing file for ' + id, err);
          }

          console.log('content success: ' + id);
          next();
        });
      });
    },
    processDone: function() {
      processImageInfo();
    }
  });
};

// ----------
var processImageInfo = function() {
  new LineReader({
    name: 'input/ImageInfo.txt',
    processLine: function(line, next) {
      var parts = line.split('\t');
      if (parts.length <= 1) {
        next();
        return;
      }

      var id = parts[4];
      if (!id) {
        console.log('bad ID: ' + line);
        next();
        return;
      }

      if (id === 'Id') {
        next();
        return;
      }

      id = id.replace(/([A-Z])/g, '_$1');

      fs.readFile('output/' + id + '.json', 'utf8', function(err, data2) {
        if (err) {
          if (err.code !== 'ENOENT') {
            console.log('readError', err);
          }

          next();
          return;
        }

        var info;
        try {
          info = JSON.parse(data2);
        } catch (e) {
          console.log('exception reading', id, e);
        }

        if (!info) {
          next();
          return;
        }

        info.height = parseInt(parts[3], 10);
        info.tileFormat = parts[5];
        info.tileOverlap = parts[6];
        info.tileSize = parts[7];
        info.width = parseInt(parts[9], 10);

        // console.log(JSON.stringify(info, null, 2));
        fs.writeFile('output/' + id + '.json', JSON.stringify(info, null, 2), function(err) {
          if (err) {
            console.log('error writing file for ' + id, err);
          }

          console.log('image success: ' + id);
          next();
        });
      });
    },
    processDone: function() {
      console.log('all done!');
    }
  });
};

// ----------
var withFolder = function(path, next) {
  fs.exists(path, function(exists) {
    if (exists) {
      next();
    } else {
      fs.mkdir(path, function(err) {
        next();
      });
    }
  });
};

// ----------
var start = function() {
  // Uncomment one of these to select it for running:
  // withFolder('output', processContentInfo);
  // withFolder('output', processImageInfo);
  // withFolder('output2', processAnalytics);
};

// ----------
start();
