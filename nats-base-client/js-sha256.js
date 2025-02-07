// deno-fmt-ignore-file
// deno-lint-ignore-file
// This code was bundled using `deno bundle` and it's not recommended to edit it manually
// https://github.com/emn178/js-sha256 (MIT)

function t(t, e) {
    return e.forEach(function(e) {
        e && "string" != typeof e && !Array.isArray(e) && Object.keys(e).forEach(function(r) {
            if ("default" !== r && !(r in t)) {
                var i = Object.getOwnPropertyDescriptor(e, r);
                Object.defineProperty(t, r, i.get ? i : {
                    enumerable: !0,
                    get: function() {
                        return e[r];
                    }
                });
            }
        });
    }), Object.freeze(t);
}
var e = "undefined" != typeof global ? global : "undefined" != typeof self ? self : "undefined" != typeof window ? window : {};
function r() {
    throw new Error("setTimeout has not been defined");
}
function i() {
    throw new Error("clearTimeout has not been defined");
}
var h = r, s = i;
function n(t) {
    if (h === setTimeout) return setTimeout(t, 0);
    if ((h === r || !h) && setTimeout) return h = setTimeout, setTimeout(t, 0);
    try {
        return h(t, 0);
    } catch (e) {
        try {
            return h.call(null, t, 0);
        } catch (e) {
            return h.call(this, t, 0);
        }
    }
}
"function" == typeof e.setTimeout && (h = setTimeout), "function" == typeof e.clearTimeout && (s = clearTimeout);
var o, a = [], f = !1, u = -1;
function c() {
    f && o && (f = !1, o.length ? a = o.concat(a) : u = -1, a.length && l());
}
function l() {
    if (!f) {
        var t = n(c);
        f = !0;
        for(var e = a.length; e;){
            for(o = a, a = []; ++u < e;)o && o[u].run();
            u = -1, e = a.length;
        }
        o = null, f = !1, function(t) {
            if (s === clearTimeout) return clearTimeout(t);
            if ((s === i || !s) && clearTimeout) return s = clearTimeout, clearTimeout(t);
            try {
                return s(t);
            } catch (e) {
                try {
                    return s.call(null, t);
                } catch (e) {
                    return s.call(this, t);
                }
            }
        }(t);
    }
}
function y(t, e) {
    this.fun = t, this.array = e;
}
y.prototype.run = function() {
    this.fun.apply(null, this.array);
};
function p() {}
var d = p, w = p, b = p, v = p, A = p, g = p, _ = p;
var m = e.performance || {}, O = m.now || m.mozNow || m.msNow || m.oNow || m.webkitNow || function() {
    return (new Date).getTime();
};
var B = new Date;
var E = {
    nextTick: function(t) {
        var e = new Array(arguments.length - 1);
        if (arguments.length > 1) for(var r = 1; r < arguments.length; r++)e[r - 1] = arguments[r];
        a.push(new y(t, e)), 1 !== a.length || f || n(l);
    },
    title: "browser",
    browser: !0,
    env: {},
    argv: [],
    version: "",
    versions: {},
    on: d,
    addListener: w,
    once: b,
    off: v,
    removeListener: A,
    removeAllListeners: g,
    emit: _,
    binding: function(t) {
        throw new Error("process.binding is not supported");
    },
    cwd: function() {
        return "/";
    },
    chdir: function(t) {
        throw new Error("process.chdir is not supported");
    },
    umask: function() {
        return 0;
    },
    hrtime: function(t) {
        var e = .001 * O.call(m), r = Math.floor(e), i = Math.floor(e % 1 * 1e9);
        return t && (r -= t[0], (i -= t[1]) < 0 && (r--, i += 1e9)), [
            r,
            i
        ];
    },
    platform: "browser",
    release: {},
    config: {},
    uptime: function() {
        return (new Date - B) / 1e3;
    }
}, S = "undefined" != typeof globalThis ? globalThis : "undefined" != typeof window ? window : "undefined" != typeof global ? global : "undefined" != typeof self ? self : {};
function T(t) {
    if (t.__esModule) return t;
    var e = Object.defineProperty({}, "__esModule", {
        value: !0
    });
    return Object.keys(t).forEach(function(r) {
        var i = Object.getOwnPropertyDescriptor(t, r);
        Object.defineProperty(e, r, i.get ? i : {
            enumerable: !0,
            get: function() {
                return t[r];
            }
        });
    }), e;
}
var k, x = {
    exports: {}
}, j = {}, N = T(t({
    __proto__: null,
    default: j
}, [
    j
]));
k = x, function() {
    var t = "input is invalid type", e = "object" == typeof window, r = e ? window : {};
    r.JS_SHA256_NO_WINDOW && (e = !1);
    var i = !e && "object" == typeof self, h = !r.JS_SHA256_NO_NODE_JS && E.versions && E.versions.node;
    h ? r = S : i && (r = self);
    var s = !r.JS_SHA256_NO_COMMON_JS && k.exports, n = !r.JS_SHA256_NO_ARRAY_BUFFER && "undefined" != typeof ArrayBuffer, o = "0123456789abcdef".split(""), a = [
        -2147483648,
        8388608,
        32768,
        128
    ], f = [
        24,
        16,
        8,
        0
    ], u = [
        1116352408,
        1899447441,
        3049323471,
        3921009573,
        961987163,
        1508970993,
        2453635748,
        2870763221,
        3624381080,
        310598401,
        607225278,
        1426881987,
        1925078388,
        2162078206,
        2614888103,
        3248222580,
        3835390401,
        4022224774,
        264347078,
        604807628,
        770255983,
        1249150122,
        1555081692,
        1996064986,
        2554220882,
        2821834349,
        2952996808,
        3210313671,
        3336571891,
        3584528711,
        113926993,
        338241895,
        666307205,
        773529912,
        1294757372,
        1396182291,
        1695183700,
        1986661051,
        2177026350,
        2456956037,
        2730485921,
        2820302411,
        3259730800,
        3345764771,
        3516065817,
        3600352804,
        4094571909,
        275423344,
        430227734,
        506948616,
        659060556,
        883997877,
        958139571,
        1322822218,
        1537002063,
        1747873779,
        1955562222,
        2024104815,
        2227730452,
        2361852424,
        2428436474,
        2756734187,
        3204031479,
        3329325298
    ], c = [
        "hex",
        "array",
        "digest",
        "arrayBuffer"
    ], l = [];
    !r.JS_SHA256_NO_NODE_JS && Array.isArray || (Array.isArray = function(t) {
        return "[object Array]" === Object.prototype.toString.call(t);
    }), !n || !r.JS_SHA256_NO_ARRAY_BUFFER_IS_VIEW && ArrayBuffer.isView || (ArrayBuffer.isView = function(t) {
        return "object" == typeof t && t.buffer && t.buffer.constructor === ArrayBuffer;
    });
    var y = function(t, e) {
        return function(r) {
            return new v(e, !0).update(r)[t]();
        };
    }, p = function(t) {
        var e = y("hex", t);
        h && (e = d(e, t)), e.create = function() {
            return new v(t);
        }, e.update = function(t) {
            return e.create().update(t);
        };
        for(var r = 0; r < c.length; ++r){
            var i = c[r];
            e[i] = y(i, t);
        }
        return e;
    }, d = function(e, i) {
        var h, s = N, n = N.Buffer, o = i ? "sha224" : "sha256";
        return h = n.from && !r.JS_SHA256_NO_BUFFER_FROM ? n.from : function(t) {
            return new n(t);
        }, function(r) {
            if ("string" == typeof r) return s.createHash(o).update(r, "utf8").digest("hex");
            if (null == r) throw new Error(t);
            return r.constructor === ArrayBuffer && (r = new Uint8Array(r)), Array.isArray(r) || ArrayBuffer.isView(r) || r.constructor === n ? s.createHash(o).update(h(r)).digest("hex") : e(r);
        };
    }, w = function(t, e) {
        return function(r, i) {
            return new A(r, e, !0).update(i)[t]();
        };
    }, b = function(t) {
        var e = w("hex", t);
        e.create = function(e) {
            return new A(e, t);
        }, e.update = function(t, r) {
            return e.create(t).update(r);
        };
        for(var r = 0; r < c.length; ++r){
            var i = c[r];
            e[i] = w(i, t);
        }
        return e;
    };
    function v(t, e) {
        e ? (l[0] = l[16] = l[1] = l[2] = l[3] = l[4] = l[5] = l[6] = l[7] = l[8] = l[9] = l[10] = l[11] = l[12] = l[13] = l[14] = l[15] = 0, this.blocks = l) : this.blocks = [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
        ], t ? (this.h0 = 3238371032, this.h1 = 914150663, this.h2 = 812702999, this.h3 = 4144912697, this.h4 = 4290775857, this.h5 = 1750603025, this.h6 = 1694076839, this.h7 = 3204075428) : (this.h0 = 1779033703, this.h1 = 3144134277, this.h2 = 1013904242, this.h3 = 2773480762, this.h4 = 1359893119, this.h5 = 2600822924, this.h6 = 528734635, this.h7 = 1541459225), this.block = this.start = this.bytes = this.hBytes = 0, this.finalized = this.hashed = !1, this.first = !0, this.is224 = t;
    }
    function A(e, r, i) {
        var h, s = typeof e;
        if ("string" === s) {
            var o, a = [], f = e.length, u = 0;
            for(h = 0; h < f; ++h)(o = e.charCodeAt(h)) < 128 ? a[u++] = o : o < 2048 ? (a[u++] = 192 | o >>> 6, a[u++] = 128 | 63 & o) : o < 55296 || o >= 57344 ? (a[u++] = 224 | o >>> 12, a[u++] = 128 | o >>> 6 & 63, a[u++] = 128 | 63 & o) : (o = 65536 + ((1023 & o) << 10 | 1023 & e.charCodeAt(++h)), a[u++] = 240 | o >>> 18, a[u++] = 128 | o >>> 12 & 63, a[u++] = 128 | o >>> 6 & 63, a[u++] = 128 | 63 & o);
            e = a;
        } else {
            if ("object" !== s) throw new Error(t);
            if (null === e) throw new Error(t);
            if (n && e.constructor === ArrayBuffer) e = new Uint8Array(e);
            else if (!(Array.isArray(e) || n && ArrayBuffer.isView(e))) throw new Error(t);
        }
        e.length > 64 && (e = new v(r, !0).update(e).array());
        var c = [], l = [];
        for(h = 0; h < 64; ++h){
            var y = e[h] || 0;
            c[h] = 92 ^ y, l[h] = 54 ^ y;
        }
        v.call(this, r, i), this.update(l), this.oKeyPad = c, this.inner = !0, this.sharedMemory = i;
    }
    v.prototype.update = function(e) {
        if (!this.finalized) {
            var r, i = typeof e;
            if ("string" !== i) {
                if ("object" !== i) throw new Error(t);
                if (null === e) throw new Error(t);
                if (n && e.constructor === ArrayBuffer) e = new Uint8Array(e);
                else if (!(Array.isArray(e) || n && ArrayBuffer.isView(e))) throw new Error(t);
                r = !0;
            }
            for(var h, s, o = 0, a = e.length, u = this.blocks; o < a;){
                if (this.hashed && (this.hashed = !1, u[0] = this.block, this.block = u[16] = u[1] = u[2] = u[3] = u[4] = u[5] = u[6] = u[7] = u[8] = u[9] = u[10] = u[11] = u[12] = u[13] = u[14] = u[15] = 0), r) for(s = this.start; o < a && s < 64; ++o)u[s >>> 2] |= e[o] << f[3 & s++];
                else for(s = this.start; o < a && s < 64; ++o)(h = e.charCodeAt(o)) < 128 ? u[s >>> 2] |= h << f[3 & s++] : h < 2048 ? (u[s >>> 2] |= (192 | h >>> 6) << f[3 & s++], u[s >>> 2] |= (128 | 63 & h) << f[3 & s++]) : h < 55296 || h >= 57344 ? (u[s >>> 2] |= (224 | h >>> 12) << f[3 & s++], u[s >>> 2] |= (128 | h >>> 6 & 63) << f[3 & s++], u[s >>> 2] |= (128 | 63 & h) << f[3 & s++]) : (h = 65536 + ((1023 & h) << 10 | 1023 & e.charCodeAt(++o)), u[s >>> 2] |= (240 | h >>> 18) << f[3 & s++], u[s >>> 2] |= (128 | h >>> 12 & 63) << f[3 & s++], u[s >>> 2] |= (128 | h >>> 6 & 63) << f[3 & s++], u[s >>> 2] |= (128 | 63 & h) << f[3 & s++]);
                this.lastByteIndex = s, this.bytes += s - this.start, s >= 64 ? (this.block = u[16], this.start = s - 64, this.hash(), this.hashed = !0) : this.start = s;
            }
            return this.bytes > 4294967295 && (this.hBytes += this.bytes / 4294967296 | 0, this.bytes = this.bytes % 4294967296), this;
        }
    }, v.prototype.finalize = function() {
        if (!this.finalized) {
            this.finalized = !0;
            var t = this.blocks, e = this.lastByteIndex;
            t[16] = this.block, t[e >>> 2] |= a[3 & e], this.block = t[16], e >= 56 && (this.hashed || this.hash(), t[0] = this.block, t[16] = t[1] = t[2] = t[3] = t[4] = t[5] = t[6] = t[7] = t[8] = t[9] = t[10] = t[11] = t[12] = t[13] = t[14] = t[15] = 0), t[14] = this.hBytes << 3 | this.bytes >>> 29, t[15] = this.bytes << 3, this.hash();
        }
    }, v.prototype.hash = function() {
        var t, e, r, i, h, s, n, o, a, f = this.h0, c = this.h1, l = this.h2, y = this.h3, p = this.h4, d = this.h5, w = this.h6, b = this.h7, v = this.blocks;
        for(t = 16; t < 64; ++t)e = ((h = v[t - 15]) >>> 7 | h << 25) ^ (h >>> 18 | h << 14) ^ h >>> 3, r = ((h = v[t - 2]) >>> 17 | h << 15) ^ (h >>> 19 | h << 13) ^ h >>> 10, v[t] = v[t - 16] + e + v[t - 7] + r | 0;
        for(a = c & l, t = 0; t < 64; t += 4)this.first ? (this.is224 ? (s = 300032, b = (h = v[0] - 1413257819) - 150054599 | 0, y = h + 24177077 | 0) : (s = 704751109, b = (h = v[0] - 210244248) - 1521486534 | 0, y = h + 143694565 | 0), this.first = !1) : (e = (f >>> 2 | f << 30) ^ (f >>> 13 | f << 19) ^ (f >>> 22 | f << 10), i = (s = f & c) ^ f & l ^ a, b = y + (h = b + (r = (p >>> 6 | p << 26) ^ (p >>> 11 | p << 21) ^ (p >>> 25 | p << 7)) + (p & d ^ ~p & w) + u[t] + v[t]) | 0, y = h + (e + i) | 0), e = (y >>> 2 | y << 30) ^ (y >>> 13 | y << 19) ^ (y >>> 22 | y << 10), i = (n = y & f) ^ y & c ^ s, w = l + (h = w + (r = (b >>> 6 | b << 26) ^ (b >>> 11 | b << 21) ^ (b >>> 25 | b << 7)) + (b & p ^ ~b & d) + u[t + 1] + v[t + 1]) | 0, e = ((l = h + (e + i) | 0) >>> 2 | l << 30) ^ (l >>> 13 | l << 19) ^ (l >>> 22 | l << 10), i = (o = l & y) ^ l & f ^ n, d = c + (h = d + (r = (w >>> 6 | w << 26) ^ (w >>> 11 | w << 21) ^ (w >>> 25 | w << 7)) + (w & b ^ ~w & p) + u[t + 2] + v[t + 2]) | 0, e = ((c = h + (e + i) | 0) >>> 2 | c << 30) ^ (c >>> 13 | c << 19) ^ (c >>> 22 | c << 10), i = (a = c & l) ^ c & y ^ o, p = f + (h = p + (r = (d >>> 6 | d << 26) ^ (d >>> 11 | d << 21) ^ (d >>> 25 | d << 7)) + (d & w ^ ~d & b) + u[t + 3] + v[t + 3]) | 0, f = h + (e + i) | 0, this.chromeBugWorkAround = !0;
        this.h0 = this.h0 + f | 0, this.h1 = this.h1 + c | 0, this.h2 = this.h2 + l | 0, this.h3 = this.h3 + y | 0, this.h4 = this.h4 + p | 0, this.h5 = this.h5 + d | 0, this.h6 = this.h6 + w | 0, this.h7 = this.h7 + b | 0;
    }, v.prototype.hex = function() {
        this.finalize();
        var t = this.h0, e = this.h1, r = this.h2, i = this.h3, h = this.h4, s = this.h5, n = this.h6, a = this.h7, f = o[t >>> 28 & 15] + o[t >>> 24 & 15] + o[t >>> 20 & 15] + o[t >>> 16 & 15] + o[t >>> 12 & 15] + o[t >>> 8 & 15] + o[t >>> 4 & 15] + o[15 & t] + o[e >>> 28 & 15] + o[e >>> 24 & 15] + o[e >>> 20 & 15] + o[e >>> 16 & 15] + o[e >>> 12 & 15] + o[e >>> 8 & 15] + o[e >>> 4 & 15] + o[15 & e] + o[r >>> 28 & 15] + o[r >>> 24 & 15] + o[r >>> 20 & 15] + o[r >>> 16 & 15] + o[r >>> 12 & 15] + o[r >>> 8 & 15] + o[r >>> 4 & 15] + o[15 & r] + o[i >>> 28 & 15] + o[i >>> 24 & 15] + o[i >>> 20 & 15] + o[i >>> 16 & 15] + o[i >>> 12 & 15] + o[i >>> 8 & 15] + o[i >>> 4 & 15] + o[15 & i] + o[h >>> 28 & 15] + o[h >>> 24 & 15] + o[h >>> 20 & 15] + o[h >>> 16 & 15] + o[h >>> 12 & 15] + o[h >>> 8 & 15] + o[h >>> 4 & 15] + o[15 & h] + o[s >>> 28 & 15] + o[s >>> 24 & 15] + o[s >>> 20 & 15] + o[s >>> 16 & 15] + o[s >>> 12 & 15] + o[s >>> 8 & 15] + o[s >>> 4 & 15] + o[15 & s] + o[n >>> 28 & 15] + o[n >>> 24 & 15] + o[n >>> 20 & 15] + o[n >>> 16 & 15] + o[n >>> 12 & 15] + o[n >>> 8 & 15] + o[n >>> 4 & 15] + o[15 & n];
        return this.is224 || (f += o[a >>> 28 & 15] + o[a >>> 24 & 15] + o[a >>> 20 & 15] + o[a >>> 16 & 15] + o[a >>> 12 & 15] + o[a >>> 8 & 15] + o[a >>> 4 & 15] + o[15 & a]), f;
    }, v.prototype.toString = v.prototype.hex, v.prototype.digest = function() {
        this.finalize();
        var t = this.h0, e = this.h1, r = this.h2, i = this.h3, h = this.h4, s = this.h5, n = this.h6, o = this.h7, a = [
            t >>> 24 & 255,
            t >>> 16 & 255,
            t >>> 8 & 255,
            255 & t,
            e >>> 24 & 255,
            e >>> 16 & 255,
            e >>> 8 & 255,
            255 & e,
            r >>> 24 & 255,
            r >>> 16 & 255,
            r >>> 8 & 255,
            255 & r,
            i >>> 24 & 255,
            i >>> 16 & 255,
            i >>> 8 & 255,
            255 & i,
            h >>> 24 & 255,
            h >>> 16 & 255,
            h >>> 8 & 255,
            255 & h,
            s >>> 24 & 255,
            s >>> 16 & 255,
            s >>> 8 & 255,
            255 & s,
            n >>> 24 & 255,
            n >>> 16 & 255,
            n >>> 8 & 255,
            255 & n
        ];
        return this.is224 || a.push(o >>> 24 & 255, o >>> 16 & 255, o >>> 8 & 255, 255 & o), a;
    }, v.prototype.array = v.prototype.digest, v.prototype.arrayBuffer = function() {
        this.finalize();
        var t = new ArrayBuffer(this.is224 ? 28 : 32), e = new DataView(t);
        return e.setUint32(0, this.h0), e.setUint32(4, this.h1), e.setUint32(8, this.h2), e.setUint32(12, this.h3), e.setUint32(16, this.h4), e.setUint32(20, this.h5), e.setUint32(24, this.h6), this.is224 || e.setUint32(28, this.h7), t;
    }, A.prototype = new v, A.prototype.finalize = function() {
        if (v.prototype.finalize.call(this), this.inner) {
            this.inner = !1;
            var t = this.array();
            v.call(this, this.is224, this.sharedMemory), this.update(this.oKeyPad), this.update(t), v.prototype.finalize.call(this);
        }
    };
    var g = p();
    g.sha256 = g, g.sha224 = p(!0), g.sha256.hmac = b(), g.sha224.hmac = b(!0), s ? k.exports = g : (r.sha256 = g.sha256, r.sha224 = g.sha224);
}();
var U = x.exports, z = x.exports.sha224, J = x.exports.sha256;
export { U as default, z as sha224, J as sha256 };
