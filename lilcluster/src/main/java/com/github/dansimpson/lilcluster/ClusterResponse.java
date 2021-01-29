package com.github.dansimpson.lilcluster;

import java.util.List;

/**
 * A collection of peer responses with some helper functions around asserting total success.
 * 
 * @author Dan Simpson
 *
 */
public class ClusterResponse {

	private final List<PeerResponse> responses;

	/**
	 * @param responses
	 */
	public ClusterResponse(List<PeerResponse> responses) {
		super();
		this.responses = responses;
	}

	/**
	 * @return the responses
	 */
	public List<PeerResponse> getResponses() {
		return responses;
	}

	public boolean isSuccess() {
		return !isError();
	}

	public boolean isError() {
		return responses.stream().anyMatch(PeerResponse::isError);
	}
}
