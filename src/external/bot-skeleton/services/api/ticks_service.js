/* eslint-disable no-confusing-arrow */
import { getLast, historyToTicks } from '../../utils/binary-utils';
import { observer as globalObserver } from '../../utils/observer';
import { doUntilDone, getUUID } from '../tradeEngine/utils/helpers';
import { api_base } from './api-base';

const parseTick = tick => ({
    epoch: +tick.epoch,
    quote: +tick.quote,
});

const parseOhlc = ohlc => ({
    open: +ohlc.open,
    high: +ohlc.high,
    low: +ohlc.low,
    close: +ohlc.close,
    epoch: +(ohlc.open_time || ohlc.epoch),
});

const parseCandles = candles => candles.map(t => parseOhlc(t));

const updateTicks = (ticks, newTick) => (getLast(ticks).epoch >= newTick.epoch ? ticks : [...ticks.slice(1), newTick]);

const updateCandles = (candles, ohlc) => {
    const lastCandle = getLast(candles);
    if (
        (lastCandle.open === ohlc.open &&
            lastCandle.high === ohlc.high &&
            lastCandle.low === ohlc.low &&
            lastCandle.close === ohlc.close &&
            lastCandle.epoch === ohlc.epoch) ||
        lastCandle.epoch > ohlc.epoch
    ) {
        return candles;
    }
    const prevCandles = lastCandle.epoch === ohlc.epoch ? candles.slice(0, -1) : candles.slice(1);
    return [...prevCandles, ohlc];
};

const getType = isCandle => (isCandle ? 'candles' : 'ticks');

// Helper functions to replace immutable.Map nested operations
const getNestedValue = (obj, keys) => {
    let current = obj;
    for (const key of keys) {
        if (current === undefined || current === null) return undefined;
        current = current[key];
    }
    return current;
};

const setNestedValue = (obj, keys, value) => {
    const newObj = JSON.parse(JSON.stringify(obj));
    let current = newObj;
    for (let i = 0; i < keys.length - 1; i++) {
        if (current[keys[i]] === undefined) {
            current[keys[i]] = {};
        }
        current = current[keys[i]];
    }
    current[keys[keys.length - 1]] = value;
    return newObj;
};

const deleteNestedValue = (obj, keys) => {
    const newObj = JSON.parse(JSON.stringify(obj));
    let current = newObj;
    for (let i = 0; i < keys.length - 1; i++) {
        if (current[keys[i]] === undefined) return newObj;
        current = current[keys[i]];
    }
    delete current[keys[keys.length - 1]];
    return newObj;
};

export default class TicksService {
    constructor() {
        this.ticks = {};
        this.candles = {};
        this.tickListeners = {};
        this.ohlcListeners = {};
        this.subscriptions = {};
        this.ticks_history_promise = null;
        this.active_symbols_promise = null;
        this.candles_promise = null;

        this.observe();
    }

    requestPipSizes() {
        if (this.pipSizes) {
            return Promise.resolve(this.pipSizes);
        }

        if (!this.active_symbols_promise) {
            this.active_symbols_promise = new Promise(resolve => {
                this.pipSizes = api_base.pip_sizes;
                resolve(this.pipSizes);
            });
        }
        return this.active_symbols_promise;
    }

    async request(options) {
        return new Promise((resolve, reject) => {
            const { symbol, granularity } = options;

            const style = getType(granularity);

            if (style === 'ticks' && symbol in this.ticks) {
                resolve(this.ticks[symbol]);
            }

            if (style === 'candles' && getNestedValue(this.candles, [symbol, Number(granularity)]) !== undefined) {
                resolve(getNestedValue(this.candles, [symbol, Number(granularity)]));
            }
            this.requestStream({ ...options, style })
                .then(res => {
                    resolve(res);
                })
                .catch(e => {
                    reject(e);
                });
        });
    }

    monitor(options) {
        return new Promise((resolve, reject) => {
            const { symbol, granularity, callback } = options;

            const type = getType(granularity);

            const key = getUUID();
            this.request(options)
                .then(() => {
                    if (type === 'ticks') {
                        this.tickListeners = setNestedValue(this.tickListeners, [symbol, key], callback);
                        globalObserver.emit('bot.bot_ready');
                        api_base.toggleRunButton(false);
                    } else {
                        this.ohlcListeners = setNestedValue(this.ohlcListeners, [symbol, Number(granularity), key], callback);
                    }
                    resolve(key);
                })
                .catch(e => {
                    globalObserver.emit('Error', e);
                    this.ticks_history_promise = null;
                    api_base.toggleRunButton(false);
                    reject(e);
                });
        });
    }

    async stopMonitor(options) {
        const { symbol, granularity, key } = options;
        const type = getType(granularity);

        if (type === 'ticks' && getNestedValue(this.tickListeners, [symbol, key]) !== undefined) {
            this.tickListeners = deleteNestedValue(this.tickListeners, [symbol, key]);
        }

        if (type === 'candles' && getNestedValue(this.ohlcListeners, [symbol, Number(granularity), key]) !== undefined) {
            this.ohlcListeners = deleteNestedValue(this.ohlcListeners, [symbol, Number(granularity), key]);
        }

        await this.unsubscribeIfEmptyListeners(options);
    }

    async unsubscribeIfEmptyListeners(options) {
        const { symbol, granularity } = options;

        let needToUnsubscribe = false;

        const tickListener = this.tickListeners[symbol];

        if (tickListener && Object.keys(tickListener).length === 0) {
            this.tickListeners = deleteNestedValue(this.tickListeners, [symbol]);
            this.ticks = deleteNestedValue(this.ticks, [symbol]);
            needToUnsubscribe = true;
        }

        const ohlcListener = getNestedValue(this.ohlcListeners, [symbol, Number(granularity)]);

        if (ohlcListener && Object.keys(ohlcListener).length === 0) {
            this.ohlcListeners = deleteNestedValue(this.ohlcListeners, [symbol, Number(granularity)]);
            this.candles = deleteNestedValue(this.candles, [symbol, Number(granularity)]);
            needToUnsubscribe = true;
        }

        if (needToUnsubscribe) {
            await this.unsubscribeAllAndSubscribeListeners(symbol);
        }
    }

    unsubscribeAllAndSubscribeListeners(symbol) {
        const ohlcSubscriptions = getNestedValue(this.subscriptions, ['ohlc', symbol]);

        const subscription = [...(ohlcSubscriptions ? Object.values(ohlcSubscriptions) : [])];

        Promise.all(subscription.map(id => doUntilDone(() => api_base.api.forget(id))));

        this.subscriptions = {};
    }

    updateTicksAndCallListeners(symbol, ticks) {
        if (this.ticks[symbol] === ticks) {
            return;
        }
        this.ticks[symbol] = ticks;

        const listeners = this.tickListeners[symbol];

        if (listeners) {
            Object.values(listeners).forEach(callback => callback(this.ticks[symbol]));
        }
    }

    updateCandlesAndCallListeners(address, candles) {
        if (getNestedValue(this.ticks, address) === candles) {
            return;
        }
        this.candles = setNestedValue(this.candles, address, candles);

        const listeners = getNestedValue(this.ohlcListeners, address);

        if (listeners) {
            Object.values(listeners).forEach(callback => callback(getNestedValue(this.candles, address)));
        }
    }

    observe() {
        if (api_base.api) {
            const subscription = api_base.api.onMessage().subscribe(({ data }) => {
                if (data.msg_type === 'tick') {
                    const { tick } = data;
                    const { symbol, id } = tick;
                    if (symbol in this.ticks) {
                        this.subscriptions = setNestedValue(this.subscriptions, ['tick', symbol], id);
                        this.updateTicksAndCallListeners(symbol, updateTicks(this.ticks[symbol], parseTick(tick)));
                    }
                }

                if (data.msg_type === 'ohlc') {
                    const { ohlc } = data;
                    const { symbol, granularity, id } = ohlc;
                    if (getNestedValue(this.candles, [symbol, Number(granularity)]) !== undefined) {
                        this.subscriptions = setNestedValue(this.subscriptions, ['ohlc', symbol, Number(granularity)], id);
                        const address = [symbol, Number(granularity)];
                        this.updateCandlesAndCallListeners(
                            address,
                            updateCandles(getNestedValue(this.candles, address), parseOhlc(ohlc))
                        );
                    }
                }
            });
            api_base.pushSubscription(subscription);
        }
    }

    requestStream(options) {
        const { style } = options;
        const stringified_options = JSON.stringify(options);

        if (style === 'ticks') {
            // Check if we already have a promise for these exact options
            if (!this.ticks_history_promise || this.ticks_history_promise.stringified_options !== stringified_options) {
                this.ticks_history_promise = {
                    promise: this.requestPipSizes().then(() => this.requestTicks(options)),
                    stringified_options,
                };
            }

            return this.ticks_history_promise.promise;
        }

        if (style === 'candles') {
            // Check if we already have a promise for these exact options
            if (!this.candles_promise || this.candles_promise.stringified_options !== stringified_options) {
                this.candles_promise = {
                    promise: this.requestPipSizes().then(() => this.requestTicks(options)),
                    stringified_options,
                };
            }

            return this.candles_promise.promise;
        }

        return [];
    }

    requestTicks(options) {
        const { symbol, granularity, style } = options;
        const request_object = {
            ticks_history: symbol === 'na' ? 'R_100' : symbol,
            subscribe: 1,
            end: 'latest',
            count: 1000,
            granularity: granularity ? Number(granularity) : undefined,
            style,
        };
        return new Promise((resolve, reject) => {
            if (!api_base.api) resolve([]);
            doUntilDone(() => api_base.api.send(request_object), ['AlreadySubscribed'], api_base)
                .then(r => {
                    if (style === 'ticks') {
                        const ticks = historyToTicks(r.history);

                        this.updateTicksAndCallListeners(symbol, ticks);
                        resolve(ticks);
                    } else {
                        const candles = parseCandles(r.candles);

                        this.updateCandlesAndCallListeners([symbol, Number(granularity)], candles);

                        resolve(candles);
                    }
                })
                .catch(error => {
                    // Handle AlreadySubscribed errors gracefully - they're not fatal
                    if (error?.error?.code === 'AlreadySubscribed') {
                        // For AlreadySubscribed errors, we can still resolve with existing data
                        if (style === 'ticks' && symbol in this.ticks) {
                            resolve(this.ticks[symbol]);
                        } else if (style === 'candles' && getNestedValue(this.candles, [symbol, Number(granularity)]) !== undefined) {
                            resolve(getNestedValue(this.candles, [symbol, Number(granularity)]));
                        } else {
                            resolve([]);
                        }
                        return;
                    }
                    // Don't clear auth data for InvalidSymbol errors as it causes unwanted logouts
                    // InvalidSymbol errors can occur for various reasons and don't necessarily mean the user is unauthorized
                    reject(error);
                });
        });
    }

    forget = () => {
        return new Promise((resolve, reject) => {
            if (api_base?.api) {
                try {
                    api_base.api
                        .forgetAll('ticks')
                        .then(() => {
                            resolve();
                        })
                        .catch(reject);
                } catch (e) {
                    console.log('Error in forget ticks', e);
                }
            } else {
                resolve();
            }
        });
    };

    forgetCandleSubscription = () => {
        return new Promise((resolve, reject) => {
            if (api_base?.api) {
                try {
                    api_base.api
                        .forgetAll('candles')
                        .then(() => {
                            resolve();
                        })
                        .catch(reject);
                } catch (e) {
                    console.log('Error in forget candles', e);
                }
            } else {
                resolve();
            }
        });
    };

    unsubscribeFromTicksService() {
        return new Promise((resolve, reject) => {
            this.forget()
                .then(() => {
                    try {
                        this.forgetCandleSubscription()
                            .then(() => {
                                resolve();
                            })
                            .catch(reject);
                    } catch (e) {
                        console.log('Error in unsubscribeFromTicksService', e);
                    }
                })
                .catch(reject);
            this.ticks_history_promise = null;
        });
    }
}
