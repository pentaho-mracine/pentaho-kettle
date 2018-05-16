/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.groupby;

import org.pentaho.di.i18n.BaseMessages;

public enum GroupByType {
  NONE( 0, "-" ),
  SUM( 1, "SUM" ),
  AVERAGE( 2, "AVERAGE" ),
  MEDIAN( 3, "MEDIAN" ),
  PERCENTILE( 4, "PERCENTILE" ),
  MIN( 5, "MIN" ),
  MAX( 6, "MAX" ),
  COUNT_ALL( 7, "COUNT_ALL" ),
  CONCAT_COMMA( 8, "CONCAT_COMMA" ),
  FIRST( 9, "FIRST" ),
  LAST( 10, "LAST" ),
  FIRST_INCL_NULL( 11, "FIRST_INCL_NULL" ),
  LAST_INCL_NULL( 12, "LAST_INCL_NULL" ),
  CUMULATIVE_SUM( 13, "CUM_SUM" ),
  CUMULATIVE_AVERAGE( 14, "CUM_AVG" ),
  STANDARD_DEVIATION( 15, "STD_DEV" ),
  CONCAT_STRING( 16, "CONCAT_STRING" ),
  COUNT_DISTINCT( 17, "COUNT_DISTINCT" ),
  COUNT_ANY( 18, "COUNT_ANY" ),
  STANDARD_DEVIATION_SAMPLE( 19, "STD_DEV_SAMPLE" ),
  PERCENTILE_NEAREST_RANK( 20, "PERCENTILE_NEAREST_RANK" );

  Class<?> PKG = GroupByType.class;

  private final int type;
  private final String typeGroupCode; // short string identifier of function type (used in KTR file)

  GroupByType( int type, String typeGroupCode ) {
    this.type = type;
    this.typeGroupCode = typeGroupCode;
  }

  public final int getType() {
    return type;
  }

  public final String getTypeGroupCode() {
    return typeGroupCode;
  }

  /**
   * Retrieves the longform text of a particular group by function
   *
   * @return the text showing the function name in longform
   */
  public final String getLongDesc() {
    Class<?> PKG = GroupByType.class;
    String longDesc;

    switch( this ) {
      case SUM:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.SUM" );
        break;
      case AVERAGE:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.AVERAGE" );
        break;
      case MEDIAN:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MEDIAN" );
        break;
      case PERCENTILE:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE" );
        break;
      case MIN:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MIN" );
        break;
      case MAX:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MAX" );
        break;
      case COUNT_ALL:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ALL" );
        break;
      case CONCAT_COMMA:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_COMMA" );
        break;
      case FIRST:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST" );
        break;
      case LAST:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST" );
        break;
      case FIRST_INCL_NULL:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL" );
        break;
      case LAST_INCL_NULL:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL" );
        break;
      case CUMULATIVE_SUM:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMULATIVE_SUM" );
        break;
      case CUMULATIVE_AVERAGE:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMULATIVE_AVERAGE" );
        break;
      case STANDARD_DEVIATION:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION" );
        break;
      case CONCAT_STRING:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING" );
        break;
      case COUNT_DISTINCT:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT" );
        break;
      case COUNT_ANY:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ANY" );
        break;
      case STANDARD_DEVIATION_SAMPLE:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION_SAMPLE" );
        break;
      case PERCENTILE_NEAREST_RANK:
        longDesc = BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE_NEAREST_RANK" );
        break;
      default:
        longDesc = "-";
        break;
    }

    return longDesc;
  }

  /**
   * Returns the GroupByType given the text description of the GroupBy function
   * For example: used when a user selects a function from the group by dropdown
   * so we don't have to reference functions by its list index anymore and we can
   * rearrange the list alphabetically
   *
   * @param type the typeGroupCode OR long description of the group by function
   * @return the enumeration of the function
   */
  public static GroupByType getTypeFromString( String type ) {
    Class<?> PKG = GroupByType.class;

    if ( type.equals( AVERAGE.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.AVERAGE" ) ) )
      return AVERAGE;
    else if ( type.equals( CONCAT_STRING.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING" ) ) )
      return CONCAT_STRING;
    else if ( type.equals( CONCAT_COMMA.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_COMMA" ) ) )
      return CONCAT_COMMA;
    else if ( type.equals( CUMULATIVE_AVERAGE.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMULATIVE_AVERAGE" ) ) )
      return CUMULATIVE_AVERAGE;
    else if ( type.equals( CUMULATIVE_SUM.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMULATIVE_SUM" ) ) )
      return CUMULATIVE_SUM;
    else if ( type.equals( FIRST.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST" ) ) )
      return FIRST;
    else if ( type.equals( FIRST_INCL_NULL.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL" ) ) )
      return FIRST_INCL_NULL;
    else if ( type.equals( LAST.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST" ) ) )
      return LAST;
    else if ( type.equals( LAST_INCL_NULL.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL" ) ) )
      return LAST_INCL_NULL;
    else if ( type.equals( MAX.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MAX" ) ) )
      return MAX;
    else if ( type.equals( MEDIAN.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MEDIAN" ) ) )
      return MEDIAN;
    else if ( type.equals( MIN.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MIN" ) ) )
      return MIN;
    else if ( type.equals( COUNT_DISTINCT.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT" ) ) )
      return COUNT_DISTINCT;
    else if ( type.equals( COUNT_ANY.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ANY" ) ) )
      return COUNT_ANY;
    else if ( type.equals( COUNT_ALL.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ALL" ) ) )
      return COUNT_ALL;
    else if ( type.equals( PERCENTILE.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE" ) ) )
      return PERCENTILE;
    else if ( type.equals( PERCENTILE_NEAREST_RANK.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE_NEAREST_RANK" ) ) )
      return PERCENTILE_NEAREST_RANK;
    else if ( type.equals( STANDARD_DEVIATION.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION" ) ) )
      return STANDARD_DEVIATION;
    else if ( type.equals( STANDARD_DEVIATION_SAMPLE.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION_SAMPLE" ) ) )
      return STANDARD_DEVIATION_SAMPLE;
    else if ( type.equals( SUM.getTypeGroupCode() ) ||
        type.equals( BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.SUM" ) ) )
      return SUM;
    else
      return NONE;
  }
}
