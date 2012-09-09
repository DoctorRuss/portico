/*
 *   Copyright 2012 The Portico Project
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
#ifndef TIMEIMPLEMENTATIONS_H_
#define TIMEIMPLEMENTATIONS_H_

#include "common.h"

IEEE1516E_NS_START

//
// In each time type, the standard IEEE-1516e headers forward declare
// a "class [TypeName]Implementation" and have that as the type for
// an "_impl" member, presumably to which the implementations of the
// standard headers will delegate. This header declares those types as
// structs, defining their makeup.
//
struct HLAfloat64TimeImpl
{
	Float64 time;
};

struct HLAfloat64IntervalImpl
{
	Float64 time;
};

struct HLAinteger64TimeImpl
{
	Integer64 time;
};

struct HLAinteger64IntervalImpl
{
	Integer64 time;
};

IEEE1516E_NS_END

#endif /* TIMEIMPLEMENTATIONS_H_ */
