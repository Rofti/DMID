package ocd.metrics.utils;

import java.util.HashMap;
import java.util.Map;


/**
 * Represents a community of a cover.
 *
 */
public class Community {

	/**
	 * A mapping from the community member (custom) nodes to their belonging factors.
	 * Belonging factors must be non-negative.
	 */
	private Map<Node, Double> memberships = new HashMap<Node, Double>();
	
	/**
	 * Creates a new instance.
	 * @param cover The cover the community belongs to.
	 */
	public Community() {
	}
	
	/**
	 * Getter for memberships.
	 * @return The memberships.
	 */
	public Map<Node, Double> getMemberships() {
		return memberships;
	}
	/**
	 * Setter for a membership entry. If the belonging factor is 0 the node is removed from the community.
	 * @param node The member node.
	 * @param belongingFactor The belonging factor.
	 */
	protected void setBelongingFactor(Node node, double belongingFactor) {
		if(belongingFactor != 0) {
			this.memberships.put(node, belongingFactor);
		}
		else
			this.memberships.remove(node);
	}
	/**
	 * Getter for the belonging factor of a certain node.
	 * @param node The member node.
	 * @return The belonging factor, i.e. the corresponding value from the
	 * memberships map or 0 if the node does not belong to the community.
	 */
	public double getBelongingFactor(Node node) {
		Double belongingFactor = this.memberships.get(node);
		if(belongingFactor == null) {
			belongingFactor = 0d;
		}
		return belongingFactor;
	}
	/**
	 * Returns the community size, i.e. the amount of community members.
	 * @return The size.
	 */
	public int getSize() {
		return this.memberships.size();
	}
	
}
