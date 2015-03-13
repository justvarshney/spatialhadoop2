/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.osm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;


public class OSMPoint extends Point {
  public long id;
  public String str="";
  public Map<String, String> tags = new HashMap<String, String>();

  @Override
  public void fromText(Text text) {
    x = TextSerializerHelper.consumeDouble(text, ',');
    y = TextSerializerHelper.consumeDouble(text, ',');
    if (text.getLength() > 0)
		str=text.toString();
	//      TextSerializerHelper.consumeMap(text, tags);
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeDouble(x, text, ',');
    TextSerializerHelper.serializeDouble(y, text, tags.isEmpty() ? '\0' : ',');
    //TextSerializerHelper.serializeMap(text, tags);
	text.append(str.getBytes(), 0, str.length());
    return text;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readLong();
    super.readFields(in);
  }
  
  @Override
  public Point clone() {
    OSMPoint c = new OSMPoint();
    c.x = x;
    c.y = y;
    c.str = new String(str);
    return c;
  }
}
