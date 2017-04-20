/*
 * Copyright 2017 resin.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const _ = require('lodash');
const stream = require('stream');
const fs = require('fs');
const fsBinding = process.binding('fs');
const FSReqWrap = fsBinding.FSReqWrap;
const debug = require('debug')('block-write-stream');

class BlockWriteStream extends stream.Writable {

  constructor(options) {

    options = Object.assign({}, BlockWriteStream.defaults, options);
    options.objectMode = true;

    // Options.highWaterMark = 32;

    debug('block-write-stream %j', options);

    super(options);

    this._writableState.highWaterMark = 1;

    this.fs = options.fs;
    this.fd = options.fd;
    this.path = options.path;
    this.flags = options.flags;
    this.mode = options.mode;
    this.autoClose = options.autoClose;

    this.position = 0;
    this.bytesRead = 0;
    this.blocksRead = 0;
    this.bytesWritten = 0;
    this.blocksWritten = 0;

    this.closed = false;
    this.destroyed = false;

    this.once('finish', function() {
      if (this.autoClose) {
        this.close();
      }
    });

    this._chunks = [];
    this._chunksLength = 0;
    this._lastPosition = 0;

    this.open();

  }

  _handleWrite(chunk, next) {

    this.blocksRead++;
    this.bytesRead += chunk.length;
    this.position = _.isNil(chunk.position) ? this.position : chunk.position;

    this.fs.write(this.fd, chunk, 0, chunk.length, this.position, (error, bytesWritten) => {
      // Debug( 'write', error || bytesWritten )
      if (error) {
        if (this.autoClose) {
          this.destroy();
        }
        next(error);
      } else {
        this.bytesWritten += bytesWritten;
        this.blocksWritten++;
        next();
      }
    });

    this.position += chunk.length;

  }

  _flushChunks(chunk, next) {

    const firstChunk = this._chunks[0];

    this.position = firstChunk && !_.isNil(firstChunk.position)
      ? firstChunk.position : this.position;

    const chunks = this._chunks;
    const position = this.position;

    // Debug('_write', 'queue:flush', this._chunksLength);

    this._chunks = [ chunk ];
    this._chunksLength = chunk.length;
    this._lastPosition = chunk.position + chunk.length;

    return BlockWriteStream.writeBuffers(
      this.fd, chunks, position, (error, bytesWritten) => {
        this.bytesWritten += bytesWritten;
        this.blocksWritten += chunks.length;
        this.position += bytesWritten;
        if (error && this.autoClose) {
          this.destroy();
        }
        next(error);
      }
    );

  }

  // TODO: Optimize the shit out of this again
  // See block-stream for example
  _write(chunk, encoding, next) {

    // Wait for file handle to be open
    if (_.isNil(this.fd)) {
      // Debug('_write:open', chunk.length, '@', chunk.address);
      this.once('open', () => {
        this._write(chunk, encoding, next);
      });
      return;
    }

    this.blocksRead++;
    this.bytesRead += chunk.length;

    // Debug('_write', chunk.length, '@', chunk.position / 512);

    if (chunk.position == null) {
      chunk.position = this._lastPosition;
    }

    if (chunk.position !== this._lastPosition) {
      // Debug('_write', 'flush:position');
      return this._flushChunks(chunk, next);
    } else if (this._chunksLength >= 64 * 1024) {
      // Debug('_write', 'flush:threshold');
      return this._flushChunks(chunk, next);
    }

    // Debug('_write', 'queue');
    this._chunks.push(chunk);
    this._chunksLength += chunk.length;
    this._lastPosition = chunk.position + chunk.length;
    return next();

  }

  open() {

    debug('open');

    if (!_.isNil(this.fd)) {
      return;
    }

    this.fs.open(this.path, this.flags, this.mode, (error, fd) => {
      if (error) {
        if (this.autoClose) {
          this.destroy();
        }
        this.emit('error', error);
      } else {
        this.fd = fd;
        this.emit('open', fd);
      }
    });

  }

  close(callback) {

    debug('close');

    if (callback) {
      this.once('close', callback);
    }

    if (this.closed || _.isNil(this.fd)) {
      if (_.isNil(this.fd)) {
        this.once('open', () => {
          this.close();
        });
      } else {
        process.nextTick(() => {
          this.emit('close');
        });
      }
      return;
    }

    this.closed = true;

    this.fs.close(this.fd, (error) => {
      if (error) {
        this.emit('error', error);
      } else {
        this.emit('close');
      }
    });

    this.fd = null;

  }

  destroy() {
    debug('destroy');
    if (this.destroyed) {
      return;
    }
    this.destroyed = true;
    this.close();
  }

}

BlockWriteStream.defaults = {
  fs,
  fd: null,
  path: null,
  flags: 'w',
  mode: 0o666,
  autoClose: true
};

BlockWriteStream.writeBuffers = function(fd, chunks, position, callback) {

  const req = new FSReqWrap();

  req.oncomplete = function(error, bytesWritten) {
    callback(error, bytesWritten || 0, chunks);
  };

  fsBinding.writeBuffers(fd, chunks, position, req);

};

module.exports = BlockWriteStream;
