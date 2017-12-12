package org.sanju.kafka.connect.beans;

import java.util.Date;

import com.arangodb.entity.BaseDocument;

/**
 *
 * @author Sanju Thomas
 *
 */
public class QuoteRequest extends BaseDocument {

	private static final long serialVersionUID = 1L;

	public QuoteRequest() {
	}

	public QuoteRequest(final String key, final String symbol, final int quantity, final Client client,
			final Date timestamp) {
		setKey(key);
		setId(key);
		this.symbol = symbol;
		this.quantity = quantity;
		this.client = client;
		this.timestamp = timestamp;
	}

	private String symbol;
	private int quantity;
	private Client client;
	private Date timestamp;

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Client getClient() {
		return this.client;
	}

	public void setClient(final Client client) {
		this.client = client;
	}

	public String getSymbol() {
		return this.symbol;
	}

	public void setSymbol(final String symbol) {
		this.symbol = symbol;
	}

	public int getQuantity() {
		return this.quantity;
	}

	public void setQuantity(final int quantity) {
		this.quantity = quantity;
	}


}
