/* jshint node: true */
"use strict";
var _ = require('lodash');
var jinst = require('./jinst');
var ResultSetMetaData = require('./resultsetmetadata');
var java = jinst.getInstance();
var winston = require('winston');
var BigNumber = require('bignumber.js');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

function ResultSet(rs) {
  this._rs = rs;
  this._holdability = (function() {
    var h = [];

    h[java.getStaticFieldValue('java.sql.ResultSet', 'CLOSE_CURSORS_AT_COMMIT')] = 'CLOSE_CURSORS_AT_COMMIT';
    h[java.getStaticFieldValue('java.sql.ResultSet', 'HOLD_CURSORS_OVER_COMMIT')] = 'HOLD_CURSORS_OVER_COMMIT';

    return h;
  })();
  this._types = (function() {
    var typeNames = [];

    typeNames[java.getStaticFieldValue("java.sql.Types", "BIT")]  = "Bit";
    typeNames[java.getStaticFieldValue("java.sql.Types", "TINYINT")]  = "Short";
    typeNames[java.getStaticFieldValue("java.sql.Types", "SMALLINT")] = "Short";
    typeNames[java.getStaticFieldValue("java.sql.Types", "INTEGER")]  = "Int";
    typeNames[java.getStaticFieldValue("java.sql.Types", "BIGINT")]   = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "FLOAT")]    = "Float";
    typeNames[java.getStaticFieldValue("java.sql.Types", "REAL")]     = "Float";
    typeNames[java.getStaticFieldValue("java.sql.Types", "DOUBLE")]   = "Double";
    typeNames[java.getStaticFieldValue("java.sql.Types", "NUMERIC")]  = "BigDecimal";
    typeNames[java.getStaticFieldValue("java.sql.Types", "DECIMAL")]  = "BigDecimal";
    typeNames[java.getStaticFieldValue("java.sql.Types", "CHAR")]     = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "VARCHAR")]     =  "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "LONGVARCHAR")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "DATE")] =  "Date";
    typeNames[java.getStaticFieldValue("java.sql.Types", "TIME")] =  "Time";
    typeNames[java.getStaticFieldValue("java.sql.Types", "TIMESTAMP")] = "Timestamp";
    typeNames[java.getStaticFieldValue("java.sql.Types", "BOOLEAN")] =  "Boolean";
    typeNames[java.getStaticFieldValue("java.sql.Types", "NCHAR")] =  "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "NVARCHAR")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "LONGNVARCHAR")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "BINARY")] = "Bytes";
    typeNames[java.getStaticFieldValue("java.sql.Types", "VARBINARY")] = "Bytes";
    typeNames[java.getStaticFieldValue("java.sql.Types", "LONGVARBINARY")] = "Bytes";
    typeNames[java.getStaticFieldValue("java.sql.Types", "BLOB")] = "Bytes";

    //                Parser      //  SQL    |  OE 11.6  |   VALUES 
    //----------------------------//---------|-----------|--------------------
    typeNames[-7] = "Bit";        // BIT     | Logical   | {true, false, null}
    typeNames[ 2] = "BigDecimal"; //Numeric  | decimal   | 99.99, 99.9999 -> Precision defined in the database
    typeNames[ 4] = "Int";        //Integer  |   int     | 123,456,789
    typeNames[12] = "String"      //varchar  |character  | character, format X(length) -> Error if it exceeds
    typeNames[91] = "Date";       //date     |   date    |
    typeNames[93] = "Timestamp"   //Timestamp|  datetm   | '99/99/9999 HH:MM:SS.SSS' as a string 
    //PROBAR
    typeNames[-5] = "String";     // bigint  |int64,recid| 
    typeNames[2004] = "Bytes"     // Blob    | blob      |
    typeNames[2005] = ""          // Clob    | clob      |
    typeNames[-3] = "Bytes"       //varbinay | raw       |
    typeNames[ 1] = "String"      //Char     |datetm-tz| '99/99/9999 HH:MM:SS.SSS' as a string 
    /* [ 'iValorExt’, *** char;*/


    return typeNames;
  })();
}

ResultSet.prototype.toObjArray = function(callback) {
  this.toObject(function(err, result) {
    if (err) return callback(err);
    callback(null, result.rows);
  });
};

ResultSet.prototype.toObject = function(callback) {
  this.toObjectIter(function(err, rs) {
    if (err) {
      return callback(err)
    } else {
      var rowIter = rs.rows;
      var rows = [];
      rowIter.nextRow(function(err, row){
        if (err) {
          return callback(err);
        }else{
          while (row && !row.done) {
            rows.push(row.value);
            rowIter.nextRow(function(err2, row2){
              if (err2) {
                return callback(err);
              }else{
                row = row2;
              };
            });
          }
        }
      });
      rs.rows = rows;
      return callback(null, rs);
    };
  });
};

ResultSet.prototype.toObjectIter = function(callback) {
  var self = this;
  self.getMetaData(function(err, rsmd) {
    if (err) {
      return callback(err);
    } else {
      var colsmetadata = [];
      rsmd.getColumnCount(function(err, colcount) {
        // Get some column metadata.
        _.each(_.range(1, colcount + 1), function(i) {
          colsmetadata.push({
            label: rsmd._rsmd.getColumnLabelSync(i),
            type: rsmd._rsmd.getColumnTypeSync(i),
            name: rsmd._rsmd.getColumnNameSync(i),
            size: rsmd._rsmd.getColumnDisplaySizeSync(i)
          });
        });

        callback(null, {
          labels: _.map(colsmetadata, 'label'),
          types: _.map(colsmetadata, 'type'),
          rows: {
            
            nextRow: function(callback) {
              try {
                var nextRow = self._rs.nextSync();
                if (! nextRow) {
                  return callback(null, {done: true});
                }
                var result = {};

                // loop through each column
                _.each(_.range(1, colcount + 1), function(i) {
                  var cmd = colsmetadata[i-1];
                  var type = self._types[cmd.type] || 'String';
                  var getter = 'get' + type + 'Sync';
                  //console.log("*****");
                  //console.log(self._types);
                  //console.log(cmd.type);
                  //console.log(getter);

                  switch(type) {
                      case 'BigDecimal':
                          var x = new BigNumber(self._rs.getBigDecimalSync(i));
                          var y = parseFloat(x);
                          result[cmd.label] = y; 
                          break;
                      case 'Bit':
                          result[cmd.label] = self._rs.getObjectSync(i);
                          break;
                      case 'Date':
                      case 'Time':
                      case 'Timestamp':
                          var dateVal = self._rs[getter](i);
                          result[cmd.label] = dateVal ? dateVal.toString() : null;
                          break;
                      case 'Float':
                          var fl = self._rs['getFloatSync'](i).toFixed(6);
                          result[cmd.label] = self._rs['getFloatSync'](i).toFixed(6);
                          break
                      default:
                          if (type === 'Int' && _.isNull(self._rs.getObjectSync(i))) {
                            result[cmd.label] = null;
                            return;
                          } else {
                            result[cmd.label] = self._rs[getter](i); 
                          };
                  }
                });

                return callback(null, {value: result, done: false});

              } catch(e) {
                return callback(e);
              };
            }
          }
        });
      });
    }
  });
};

ResultSet.prototype.close = function(callback) {
  this._rs.close(function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
};

ResultSet.prototype.getMetaData = function(callback) {
  this._rs.getMetaData(function(err, rsmd) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, new ResultSetMetaData(rsmd));
    }
  });
};

module.exports = ResultSet;
