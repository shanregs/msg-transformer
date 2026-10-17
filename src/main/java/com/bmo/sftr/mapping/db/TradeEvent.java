package com.bmo.sftr.mapping.db;

/** The stage of a row in the trade table. Only {@link #RECEIVED} rows are picked up by this application. */
public enum TradeEvent { RECEIVED, PROCESSED, PUBLISHED }
