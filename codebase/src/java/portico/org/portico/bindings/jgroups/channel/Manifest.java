/*
 *   Copyright 2015 The Portico Project
 *
 *   This file is part of portico.
 *
 *   portico is free software; you can redistribute it and/or modify
 *   it under the terms of the Common Developer and Distribution License (CDDL) 
 *   as published by Sun Microsystems. For more information see the LICENSE file.
 *   
 *   Use of this software is strictly AT YOUR OWN RISK!!!
 *   If something bad happens you do not have permission to come crying to me.
 *   (that goes for your lawyer as well)
 *
 */
package org.portico.bindings.jgroups.channel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jgroups.Address;
import org.portico.lrc.model.ObjectModel;

public class Manifest implements Serializable
{
	//----------------------------------------------------------
	//                    STATIC VARIABLES
	//----------------------------------------------------------
	private static final long serialVersionUID = 3112252018924L;

	//----------------------------------------------------------
	//                   INSTANCE VARIABLES
	//----------------------------------------------------------
	private int manifestVersion; // updated each time the manifest changes
	private String name;
	private transient Address localAddress;
	private Address coordinator;

	// The highest handle assigned to a connected member. Each joined member
	// is given a handle that then becomes their federate handle if they join.
	// Handles are allocated by the coordinator, who uses this to determine
	// the next suitable value
	private int highestHandle;

	// channel members - each is assigned a number
	private Map<Address,Integer> members;
	
	// federation information
	private ObjectModel fom;
	private Map<Address,FederateInfo> federates;

	//----------------------------------------------------------
	//                      CONSTRUCTORS
	//----------------------------------------------------------
	public Manifest( String name, Address localAddress )
	{
		this.manifestVersion = 0;
		this.name = name;
		this.localAddress = localAddress;
		this.coordinator = null;  // set after creation
		this.highestHandle = 0;
		this.members = new HashMap<Address,Integer>();
		
		// federation information
		this.fom = null; // set when someone creates a federation
		this.federates = new HashMap<Address,FederateInfo>();
		
		// record the fact that we have joined
		memberConnectedToChannel( localAddress );
	}

	//----------------------------------------------------------
	//                    INSTANCE METHODS
	//----------------------------------------------------------

	/////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////// Accessors and Mutators /////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////
	public void setLocalAddress( Address localAddress )
	{
		this.localAddress = localAddress;
	}

	public int getLocalFederateHandle()
	{
		return members.get( localAddress );
	}
	
	public String getLocalFederateName()
	{
		return federates.get(localAddress).name;
	}
	
	public String getFederateName( Address address )
	{
		return federates.get(address).name;
	}
	
	public int getFederateHandle( Address address )
	{
		return federates.get(address).handle;
	}
	
	public boolean isLocalFederateJoined()
	{
		return federates.containsKey( localAddress );
	}

	public boolean isJoinedFederate( Address address )
	{
		return federates.containsKey( address );
	}

	public Address getCoordinator()
	{
		return this.coordinator;
	}
	
	public void setCoordinator( Address coordinator )
	{
		this.coordinator = coordinator;
	}
	
	public boolean isCoordinator()
	{
		return this.coordinator.equals( this.localAddress );
	}

	public boolean isCoordinator( Address address )
	{
		return this.coordinator.equals( address );
	}

	/**
	 * @return A set containing the handles of all joined federates.
	 */
	public Set<Integer> getFederateHandles()
	{
		Set<Integer> handleSet = new HashSet<Integer>();
		for( FederateInfo federateInfo : federates.values() )
			handleSet.add( federateInfo.handle );
		
		return handleSet;
	}

	public ObjectModel getFom()
	{
		return this.fom;
	}
	
	public void setFom( ObjectModel model )
	{
		// called when a federation is first created and distributed to connections
		// when they join a channel (prior to them joining the federation). Also set
		// when a join *notification* is received if it contains FOM modules we need
		// to merge (this is done by each local federate).
		this.fom = model;
	}

	/** Return true if there is an active federation in this channel */
	public boolean containsFederation()
	{
		return this.fom != null;
	}

	public boolean containsFederate( String name )
	{
		for( FederateInfo federate : federates.values() )
		{
			if( federate.name.equals(name) )
				return true;
		}
		
		return false;
	}

	public int getManifestVersion()
	{
		return this.manifestVersion;
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// Lifecycle Notification Methods /////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////
	public synchronized void memberConnectedToChannel( Address address )
	{
		this.members.put( address, ++highestHandle );
		++manifestVersion;
	}

	public synchronized void federationCreated( ObjectModel fom )
	{
		this.fom = fom;
		++this.manifestVersion;
	}
	
	public synchronized void federateJoined( Address address, String federateName )
	{
		FederateInfo federateInfo = new FederateInfo();
		federateInfo.name = federateName;
		federateInfo.handle = members.get(address);
		this.federates.put( address, federateInfo );
		++manifestVersion;
	}

	/**
	 * Update the internal state to reflect that the federate resigned. It is still a channel
	 * member, just no longer a joined federate as well.
	 */
	public synchronized void federateResigned( Address address )
	{
		federates.remove( address );
		++manifestVersion;
	}
	
	public synchronized void federationDestroyed()
	{
		this.fom = null;
		++manifestVersion;
	}

	/**
	 * Record that the given member has left the channel. If they were the coordinator
	 * we need to decide on a new coordinator. We do this by picking the member with the
	 * lowest handle from those remaining.
	 */
	public synchronized void memberLeftChannel( Address address )
	{
		// remove the unbeliever
		members.remove( address );
		++manifestVersion;
		
		// did the coord leave? if so, figure out his next in line
		if( address.equals(this.coordinator) )
		{
			int lowest = Integer.MAX_VALUE;
			Address spartacus = null;
			for( Address currentAddress : members.keySet() )
			{
				int currentHandle = members.get( currentAddress );
				if( currentHandle < lowest )
					spartacus = currentAddress;
			}
			
			// all hail!
			this.coordinator = spartacus;
		}
	}

	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append( "\n-----------------------------------" );
		builder.append( "\nManifest: channel="+name );
		builder.append( "\n-----------------------------------" );
		builder.append( "\n Version        = "+manifestVersion );
		builder.append( "\n Local Address  = "+localAddress );
		builder.append( "\n Highest Handle = "+highestHandle );
		builder.append( "\n Is Federation  = "+containsFederation() );
		// members
		builder.append( "\n Channel members: " );
		builder.append( members.size() );
		builder.append( "\n" );
		for( Address address : members.keySet() )
		{
			// is this a federate?
			int connectionID = members.get( address );
			FederateInfo info = federates.get( address );
			if( info == null )
			{
				builder.append( "  (application) id=" );
				builder.append( connectionID );
				builder.append( ", address=" );
				builder.append( address );
				if( address.equals(coordinator) )
					builder.append( " **CO-ORDINATOR**" );
				builder.append( "\n" );
			}
			else
			{
				builder.append( "     (federate) id=" );
				builder.append( connectionID );
				builder.append( ", name=" );
				builder.append( info.name );
				builder.append( ", address=" );
				builder.append( address );
				if( address.equals(coordinator) )
					builder.append( " **CO-ORDINATOR**" );
				builder.append( "\n" );
			}
		}
		
		return builder.toString();
	}
	//----------------------------------------------------------
	//                     STATIC METHODS
	//----------------------------------------------------------

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// Private Inner Class: FedeateInfo ////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * This class stores basic information about a joined federate.
	 */
	public class FederateInfo implements Serializable
	{
		private static final long serialVersionUID = 98121116105109L;
		public int handle;
		public String name;
		public String toString(){ return name; }
	}

}
