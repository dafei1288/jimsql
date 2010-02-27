/* WB-tree File Based Associative String Data Base System.
 * Copyright (C) 1991, 1992, 1993, 2000, 2003 Free Software Foundation, Inc.
 * Copyright 2007 Clear Methods, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package net.fly78.miniSQL.library;

public class Han {
    public static Han hanMakeHan()
    {
	return new Han();
    }

    //The getter & setter methods follow the same syntax translations as
    //the corresponding methods in Entry.
    public static int han_Id(Han han)
    {
	return han.ID;
    }

    public static int han_Seg(Han han)
    {
	return han.SEG;
    }

    public static int han_Typ(Han han)
    {
	return han.TYP;
    }

    public static int han_Last(Han han)
    {
	return han.LAST;
    }

    public static int han_Wcb(Han han)
    {
	return han.WCB;
    }

    //setter
    public static void han_SetNum(Han han, int num)
    {
	han.ID = num;
    }

    public static void han_SetSeg(Han han, int seg)
    {
	han.SEG = seg;
    }

    public static void han_SetTyp(Han han, int typ)
    {
	han.TYP = typ;
    }

    public static void han_SetLast(Han han, int last)
    {
	han.LAST = last;
    }

    public static void han_SetWcb(Han han, int wcb)
    {
	han.WCB = wcb;
    }

    public int SEG, TYP;
    public int ID, LAST;
    public int WCB, SPARE;

}
