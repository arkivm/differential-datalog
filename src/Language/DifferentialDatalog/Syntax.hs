{-
Copyright (c) 2018-2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-}

{-# LANGUAGE FlexibleContexts, RecordWildCards, OverloadedStrings, LambdaCase #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

-- This module defines types used to construct the abstract syntax tree of a DDlog program:
-- Each data structure must implement several instances:
-- PP: pretty-printing
-- Eq: equality testing (in general it is recursive and it ignores the position)
-- Show: conversion to string; in general it just calls pp
-- WithPos: manipulating position information

module Language.DifferentialDatalog.Syntax (
        Attribute(..),
        ppAttributes,
        ArgType(..),
        Type(..),
        typeTypeVars,
        tBool,
        tInt,
        tDouble,
        tFloat,
        tString,
        tBit,
        tSigned,
        tStruct,
        tTuple,
        tUser,
        tVar,
        tOpaque,
        tIntern,
        tFunction,
        structFields,
        structLookupField,
        structGetField,
        structFieldGuarded,
        Field(..),
        IdentifierWithPos(..),
        identifierWithPos,
        TypeDef(..),
        tdefIsExtern,
        Constructor(..),
        consType,
        consIsUnique,
        KeyExpr(..),
        RelationRole(..),
        RelationSemantics(..),
        Relation(..),
        Index(..),
        RuleLHS(..),
        RuleRHS(..),
        rhsIsLiteral,
        rhsIsPositiveLiteral,
        rhsIsNegativeLiteral,
        rhsIsCondition,
        rhsIsFilterCondition,
        rhsIsAssignment,
        rhsIsGroupBy,
        Delay(..),
        delayZero,
        Atom(..),
        atomIsDelayed,
        Rule(..),
        ClosureExprArg(..),
        ExprNode(..),
        Expr(..),
        ENode,
        enode,
        eVar,
        eTypedVar,
        eApply,
        eApplyFunc,
        eField,
        eTupField,
        eBool,
        eTrue,
        eFalse,
        eInt,
        eFloat,
        eDouble,
        eString,
        eBit,
        eSigned,
        eStruct,
        eTuple,
        eSlice,
        eMatch,
        eVarDecl,
        eSeq,
        eITE,
        eFor,
        eSet,
        eBreak,
        eContinue,
        eReturn,
        eBinOp,
        eUnOp,
        eNot,
        ePHolder,
        eBinding,
        eTyped,
        eAs,
        eRef,
        eTry,
        eClosure,
        eFunc,
        FuncArg(..),
        argMut,
        Function(..),
        funcIsExtern,
        funcMutArgs,
        funcImmutArgs,
        funcShowProto,
        funcTypeVars,
        ModuleName(..),
        Import(..),
        HOType(..),
        hotypeIsFunction,
        hotypeIsRelation,
        hotypeTypeVars,
        HOField(..),
        Transformer(..),
        transformerTypeVars,
        Apply(..),
        DatalogProgram(..),
        emptyDatalogProgram,
        progStructs,
        progConstructors,
        progAddRel,
        progAddRules,
        progAddTypedef,
        ECtx(..),
        ctxParent)
where

import Prelude hiding((<>))
import Text.PrettyPrint
import Data.Maybe
import Data.List
import Data.Word
import qualified Data.Map as M

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.PP

-- | Meta-attributes associated with DDlog declarations
data Attribute = Attribute { attrPos  :: Pos
                           , attrName :: String
                           , attrVal  :: Expr
                           }

instance Eq Attribute where
    (==) (Attribute _ n1 v1) (Attribute _ n2 v2) = n1 == n2 && v1 == v2

instance Ord Attribute where
    compare (Attribute _ n1 v1) (Attribute _ n2 v2) = compare (n1, v1) (n2, v2)

instance WithPos Attribute where
    pos = attrPos
    atPos a p = a{attrPos = p}

instance WithName Attribute where
    name = attrName
    setName a n = a{attrName = n}

instance PP Attribute where
    pp (Attribute _ n v) = pp n <+> "=" <+> pp v

instance Show Attribute where
    show = render . pp

ppAttributes :: [Attribute] -> Doc
ppAttributes attrs = vcat $ map (\attr -> "#[" <> pp attr <> "]") attrs

data Field = Field { fieldPos  :: Pos
                   , fieldAttrs:: [Attribute]
                   , fieldName :: String
                   , fieldType :: Type
                   }

instance Eq Field where
    (==) (Field _ a1 n1 t1) (Field _ a2 n2 t2) = n1 == n2 && a1 == a2 && t1 == t2

instance Ord Field where
    compare (Field _ _ n1 t1) (Field _ _ n2 t2) = compare (n1, t1) (n2, t2)

instance WithPos Field where
    pos = fieldPos
    atPos f p = f{fieldPos = p}

instance WithName Field where
    name = fieldName
    setName f n = f { fieldName = n }

instance PP Field where
    pp (Field _ a n t) = ppAttributes a <+> pp n <> ":" <+> pp t

instance Show Field where
    show = render . pp

-- | Argument of a closure of function has type and 'mut' flag.
data ArgType = ArgType { atypePos  :: Pos
                       , atypeMut  :: Bool
                       , atypeType :: Type
                       }

instance Eq ArgType where
    (==) (ArgType _ m1 t1) (ArgType _ m2 t2) = (m1, t1) == (m2, t2)

instance Ord ArgType where
    compare (ArgType _ m1 t1) (ArgType _ m2 t2) = compare (m1, t1) (m2, t2)

instance WithPos ArgType where
    pos = atypePos
    atPos a p = a{atypePos = p}

instance PP ArgType where
    pp ArgType{..} = (if atypeMut then "mut" else empty) <+> pp atypeType

data IdentifierWithPos = IdentifierWithPos { idPos :: Pos, idName :: String }

instance Eq IdentifierWithPos where
    (==) (IdentifierWithPos _ n1) (IdentifierWithPos _ n2) = n1 == n2

instance Ord IdentifierWithPos where
    compare (IdentifierWithPos _ n1) (IdentifierWithPos _ n2) = compare n1 n2

instance WithPos IdentifierWithPos where
    pos = idPos
    atPos i p = i{idPos = p}

instance PP IdentifierWithPos where
    pp IdentifierWithPos{..} = pp idName

instance WithName IdentifierWithPos where
    name = idName
    setName i n = i{idName = n}

identifierWithPos n = IdentifierWithPos nopos n

data Type = TBool     {typePos :: Pos}
          | TInt      {typePos :: Pos}
          | TString   {typePos :: Pos}
          | TBit      {typePos :: Pos, typeWidth :: Int}
          | TSigned   {typePos :: Pos, typeWidth :: Int}
          | TDouble   {typePos :: Pos}
          | TFloat    {typePos :: Pos}
          | TStruct   {typePos :: Pos, typeCons :: [Constructor]}
          | TTuple    {typePos :: Pos, typeTupArgs :: [Type]}
          | TUser     {typePos :: Pos, typeName :: String, typeArgs :: [Type]}
          | TVar      {typePos :: Pos, tvarName :: String}
          | TOpaque   {typePos :: Pos, typeName :: String, typeArgs :: [Type]}
          | TFunction {typePos :: Pos, typeFuncArgs :: [ArgType], typeRetType :: Type}

tBool           = TBool     nopos
tInt            = TInt      nopos
tString         = TString   nopos
tBit            = TBit      nopos
tSigned         = TSigned   nopos
tDouble         = TDouble   nopos
tFloat          = TFloat    nopos
tStruct         = TStruct   nopos
tTuple [t]      = t
tTuple ts       = TTuple    nopos ts
tUser           = TUser     nopos
tVar            = TVar      nopos
tOpaque         = TOpaque   nopos
tFunction as t  = TFunction nopos as t
tIntern t       = tOpaque "internment::Intern" [t]

structGetField :: Type -> String -> Field
structGetField t f = fromJust $ structLookupField t f

structLookupField :: Type -> String -> Maybe Field
structLookupField t f = find ((==f) . name) $ structFields t

structFields :: Type -> [Field]
structFields (TStruct _ cs) = nub $ concatMap consArgs cs
structFields _              = []

-- True iff the field is not defined in some constructors
structFieldGuarded :: Type -> String -> Bool
structFieldGuarded (TStruct _ cs) f = any (isNothing . find ((==f) . name) . consArgs) cs
structFieldGuarded t              _ = error $ "structFieldGuarded " ++ show t

{-
-- All constructors that contain the field
structFieldConstructors :: [Constructor] -> String -> [Constructor]
structFieldConstructors cs f = filter (isJust . find ((==f) . name) . consArgs) cs

structTypeDef :: Refine -> Type -> TypeDef
structTypeDef r TStruct{..} = consType r $ name $ head typeCons
structTypeDef _ t           = error $ "structTypeDef " ++ show t
-}

instance Eq Type where
    (==) TBool{}              TBool{}              = True
    (==) TInt{}               TInt{}               = True
    (==) TString{}            TString{}            = True
    (==) (TBit _ w1)          (TBit _ w2)          = w1 == w2
    (==) (TSigned _ w1)       (TSigned _ w2)       = w1 == w2
    (==) (TDouble _)          (TDouble _)          = True
    (==) (TFloat _)           (TFloat _)           = True
    (==) (TStruct _ cs1)      (TStruct _ cs2)      = cs1 == cs2
    (==) (TTuple _ ts1)       (TTuple _ ts2)       = ts1 == ts2
    (==) (TUser _ n1 as1)     (TUser _ n2 as2)     = n1 == n2 && as1 == as2
    (==) (TVar _ v1)          (TVar _ v2)          = v1 == v2
    (==) (TOpaque _ t1 as1)   (TOpaque _ t2 as2)   = t1 == t2 && as1 == as2
    (==) (TFunction _ as1 r1) (TFunction _ as2 r2) = (as1, r1) == (as2, r2)
    (==) _                    _                    = False

-- assign rank to constructors; used in the implementation of Ord
trank :: Type -> Int
trank TBool  {}   = 0
trank TInt   {}   = 1
trank TString{}   = 2
trank TBit   {}   = 3
trank TSigned{}   = 4
trank TDouble{}   = 5
trank TFloat {}   = 6
trank TStruct{}   = 7
trank TTuple {}   = 8
trank TUser  {}   = 9
trank TVar   {}   =10
trank TOpaque{}   =11
trank TFunction{} =12

instance Ord Type where
    compare TBool{}              TBool{}                  = EQ
    compare TInt{}               TInt{}                   = EQ
    compare TString{}            TString{}                = EQ
    compare (TBit _ w1)          (TBit _ w2)              = compare w1 w2
    compare (TSigned _ w1)       (TSigned _ w2)           = compare w1 w2
    compare (TDouble _)          (TDouble _)              = EQ
    compare (TFloat _)           (TFloat _)               = EQ
    compare (TStruct _ cs1)      (TStruct _ cs2)          = compare cs1 cs2
    compare (TTuple _ ts1)       (TTuple _ ts2)           = compare ts1 ts2
    compare (TUser _ n1 as1)     (TUser _ n2 as2)         = compare (n1, as1) (n2, as2)
    compare (TVar _ v1)          (TVar _ v2)              = compare v1 v2
    compare (TOpaque _ t1 as1)   (TOpaque _ t2 as2)       = compare (t1, as1) (t2, as2)
    compare (TFunction _ as1 r1) (TFunction _ as2 r2)     = compare (as1, r1) (as2, r2)
    compare t1                   t2                       = compare (trank t1) (trank t2)

instance WithPos Type where
    pos = typePos
    atPos t p = t{typePos = p}

instance PP Type where
    pp (TBool _)          = "bool"
    pp (TInt _)           = "bigint"
    pp (TString _)        = "string"
    pp (TBit _ w)         = "bit<" <> pp w <> ">"
    pp (TSigned _ w)      = "signed<" <> pp w <> ">"
    pp (TDouble _)        = "double"
    pp (TFloat _)         = "float"
    pp (TStruct _ cons)   = vcat $ punctuate (" |") $ map pp cons
    pp (TTuple _ as)      = parens $ commaSep $ map pp as
    pp (TUser _ n as)     = pp n <>
                            if null as
                               then empty
                               else "<" <> (hcat $ punctuate comma $ map pp as) <> ">"
    pp (TVar _ v)         = "'" <> pp v
    pp (TOpaque _ t as)   = pp t <>
                            if null as
                               then empty
                               else "<" <> (hcat $ punctuate comma $ map pp as) <> ">"
    pp (TFunction _ as r) = "function" <> parens (commaSep $ map pp as) <> ":" <> pp r

instance Show Type where
    show = render . pp


-- | Type variables used in type declaration in the order they appear in the declaration
typeTypeVars :: Type -> [String]
typeTypeVars TBool{}       = []
typeTypeVars TInt{}        = []
typeTypeVars TString{}     = []
typeTypeVars TBit{}        = []
typeTypeVars TSigned{}     = []
typeTypeVars TDouble{}     = []
typeTypeVars TFloat{}      = []
typeTypeVars TStruct{..}   = nub $ concatMap (typeTypeVars . fieldType)
                                 $ concatMap consArgs typeCons
typeTypeVars TTuple{..}    = nub $ concatMap typeTypeVars typeTupArgs
typeTypeVars TUser{..}     = nub $ concatMap typeTypeVars typeArgs
typeTypeVars TVar{..}      = [tvarName]
typeTypeVars TOpaque{..}   = nub $ concatMap typeTypeVars typeArgs
typeTypeVars TFunction{..} = nub $ concatMap (typeTypeVars . atypeType) typeFuncArgs ++ typeTypeVars typeRetType

data TypeDef = TypeDef { tdefPos   :: Pos
                       , tdefAttrs :: [Attribute]
                       , tdefName  :: String
                       , tdefArgs  :: [String]
                       , tdefType  :: Maybe Type
                       }

instance WithPos TypeDef where
    pos = tdefPos
    atPos t p = t{tdefPos = p}

instance WithName TypeDef where
    name = tdefName
    setName d n = d{tdefName = n}

instance PP TypeDef where
    pp TypeDef{..} | isJust tdefType
                   = ppAttributes tdefAttrs $$
                     ("typedef" <+> pp tdefName <>
                      (if null tdefArgs
                          then empty
                          else "<" <> (hcat $ punctuate comma $ map (("'" <>) . pp) tdefArgs) <> ">") <+>
                      "=" <+> (pp $ fromJust tdefType))
    pp TypeDef{..} = ppAttributes tdefAttrs $$
                     ("extern type" <+> pp tdefName <>
                      (if null tdefArgs
                          then empty
                          else "<" <> (hcat $ punctuate comma $ map (("'" <>) . pp) tdefArgs) <> ">"))

instance Show TypeDef where
    show = render . pp

instance Eq TypeDef where
    (==) t1 t2 = (name t1, tdefAttrs t1, tdefAttrs t1, tdefType t1) == (name t2, tdefAttrs t2, tdefAttrs t2, tdefType t2)

-- | 'tdef' if declared as 'extern type'.
tdefIsExtern :: TypeDef -> Bool
tdefIsExtern tdef = isNothing $ tdefType tdef

data Constructor = Constructor { consPos   :: Pos
                               , consAttrs :: [Attribute]
                               , consName  :: String
                               , consArgs  :: [Field]
                               }

instance Eq Constructor where
    (==) (Constructor _ attrs1 n1 as1) (Constructor _ attrs2 n2 as2) = (n1, attrs1, as1) == (n2, attrs2, as2)

instance Ord Constructor where
    compare (Constructor _ _ n1 as1) (Constructor _ _ n2 as2) = compare (n1, as1) (n2, as2)

instance WithName Constructor where
    name = consName
    setName c n = c{consName = n}

instance WithPos Constructor where
    pos = consPos
    atPos c p = c{consPos = p}

instance PP Constructor where
    pp Constructor{..} = ppAttributes consAttrs $$
                         (pp consName <> (braces $ commaSep $ map pp consArgs))

instance Show Constructor where
    show = render . pp

consType :: DatalogProgram -> String -> TypeDef
consType d c =
    fromJust
    $ find (\td -> case tdefType td of
                        Just (TStruct _ cs) -> any ((==c) . name) cs
                        _                   -> False)
    $ progTypedefs d

-- | 'True' iff c is the unique constructor of its type
consIsUnique :: DatalogProgram -> String -> Bool
consIsUnique d c = (length $ typeCons $ fromJust $ tdefType $ consType d c) == 1

data KeyExpr = KeyExpr { keyPos  :: Pos
                       , keyVar  :: String
                       , keyExpr :: Expr
                       }

instance Eq KeyExpr where
    (==) (KeyExpr _ v1 e1) (KeyExpr _ v2 e2) = v1 == v2 && e1 == e2

instance Ord KeyExpr where
    compare (KeyExpr _ v1 e1) (KeyExpr _ v2 e2) = compare (v1, e1) (v2, e2)

instance WithPos KeyExpr where
    pos = keyPos
    atPos k p = k{keyPos = p}

instance PP KeyExpr where
    pp KeyExpr{..} = "(" <> pp keyVar <> ")" <+> pp keyExpr

instance Show KeyExpr where
    show = render . pp

data RelationRole = RelInput
                  | RelOutput
                  | RelInternal
                  deriving(Eq, Ord)

instance PP RelationRole where
    pp RelInput    = "input"
    pp RelOutput   = "output"
    pp RelInternal = empty

instance Show RelationRole where
    show = render . pp

data RelationSemantics = RelSet
                       | RelStream
                       | RelMultiset
                       deriving(Eq, Ord)

instance PP RelationSemantics where
    pp RelSet       = "relation"
    pp RelMultiset  = "multiset"
    pp RelStream    = "stream"

instance Show RelationSemantics where
    show = render . pp

data Relation = Relation { relPos        :: Pos
                         , relAttrs      :: [Attribute]
                         , relRole       :: RelationRole
                         , relSemantics  :: RelationSemantics
                         , relName       :: String
                         , relType       :: Type
                         , relPrimaryKey :: Maybe KeyExpr
                         }

instance Eq Relation where
    (==) (Relation _ a1 r1 m1 n1 t1 k1) (Relation _ a2 r2 m2 n2 t2 k2) = (a1, r1, m1, n1, t1, k1) == (a2, r2, m2, n2, t2, k2)

instance Ord Relation where
    compare (Relation _ a1 r1 m1 n1 t1 k1) (Relation _ a2 r2 m2 n2 t2 k2) =
        compare (a1, r1, m1, n1, t1, k1) (a2, r2, m2, n2, t2, k2)

instance WithPos Relation where
    pos = relPos
    atPos r p = r{relPos = p}

instance WithName Relation where
    name = relName
    setName r n = r{relName = n}

instance PP Relation where
    pp Relation{..} = (ppAttributes relAttrs) $$
                      pp relRole <+>
                      pp relSemantics <+> pp relName <+> "[" <> pp relType <> "]" <+> pkey
        where pkey = maybe empty (("primary key" <+>) . pp) relPrimaryKey

instance Show Relation where
    show = render . pp

data Index = Index { idxPos      :: Pos
                   , idxName     :: String
                   , idxVars     :: [Field]
                   , idxAtom     :: Atom
                   }

instance Eq Index where
    (==) (Index _ n1 v1 a1) (Index _ n2 v2 a2) = (n1, v1, a1) == (n2, v2, a2)

instance Ord Index where
    compare (Index _ n1 v1 a1) (Index _ n2 v2 a2) = compare (n1, v1, a1) (n2, v2, a2)

instance WithPos Index where
    pos = idxPos
    atPos i p = i {idxPos = p}

instance WithName Index where
    name = idxName
    setName i n = i{idxName = n}

instance PP Index where
    pp Index{..} = "index" <+> pp idxName <+> (parens $ commaSep $ map pp idxVars) <+>
                   "on" <+> pp idxAtom

instance Show Index where
    show = render . pp

data Delay = Delay { delayPos   :: Pos
                   , delayDelay :: Word32
                   }

instance Eq Delay where
    (==) (Delay _ d1) (Delay _ d2) = d1 == d2

instance Ord Delay where
    compare (Delay _ d1) (Delay _ d2) = compare d1 d2

instance WithPos Delay where
    pos = delayPos
    atPos d p = d {delayPos = p}

instance PP Delay where
    pp Delay{..} | delayDelay == 0 = empty
                 | otherwise       = "-" <> pp (toInteger delayDelay)

instance Show Delay where
    show = render . pp

delayZero :: Delay
delayZero = Delay nopos 0

data Atom = Atom { atomPos      :: Pos
                 , atomRelation :: String
                 , atomDelay    :: Delay
                 , atomDiff     :: Bool
                 , atomVal      :: Expr
                 }

instance Eq Atom where
    (==) (Atom _ r1 d1 df1 v1) (Atom _ r2 d2 df2 v2) = (r1, d1, df1, v1) == (r2, d2, df2, v2)

instance Ord Atom where
    compare (Atom _ r1 d1 df1 v1) (Atom _ r2 d2 df2 v2) = compare (r1, d1, df1, v1) (r2, d2, df2, v2)

instance WithPos Atom where
    pos = atomPos
    atPos a p = a{atomPos = p}

instance PP Atom where
    pp (Atom _ rel delay diff (E (EStruct _ cons as))) | rel == cons
                = pp rel <> pp delay <> (if diff then "'" else empty) <>
                  (parens $ commaSep $
                            map (\(n,e) -> (if null (name n) then empty else ("." <> pp n <> "=")) <> pp e) as)
    pp Atom{..} = pp atomRelation <> pp atomDelay <> (if atomDiff then "'" else empty) <> "[" <> pp atomVal <> "]"

instance Show Atom where
    show = render . pp

atomIsDelayed :: Atom -> Bool
atomIsDelayed a = atomDelay a /= delayZero

data RuleLHS = RuleLHS { lhsPos         :: Pos
                       , lhsAtom        :: Atom
                         -- D3log only: location to send the output record to.
                       , lhsLocation    :: Maybe Expr
                       }

instance Eq RuleLHS where
    (==) (RuleLHS _ a1 l1) (RuleLHS _ a2 l2) = (a1, l1) == (a2, l2)

instance Ord RuleLHS where
    compare (RuleLHS _ a1 l1) (RuleLHS _ a2 l2) = compare (a1, l1) (a2, l2)

instance WithPos RuleLHS where
    pos = lhsPos
    atPos l p = l{lhsPos = p}

instance PP RuleLHS where
    pp (RuleLHS _ a Nothing)  = pp a
    pp (RuleLHS _ a (Just l)) = pp a <+> "@" <> pp l

instance Show RuleLHS where
    show = render . pp

-- The RHS of a rule consists of relational atoms with
-- positive/negative polarity, Boolean conditions, aggregation,
-- disaggregation (flatmap), inspect operations.
data RuleRHS = RHSLiteral   {rhsPolarity :: Bool, rhsAtom :: Atom}
               -- Expression that can filter or map input collation, or both:
               -- * Filtering: `x == y`
               -- * Mapping: `var x = f(y)`
               -- * Filter/map: `Some{x} = f(y)`
             | RHSCondition {
                   rhsExpr :: Expr
               }
               -- Group input records:
               -- 'var <rhsVar> = <rhsProject>.group_by(<rhsGroupBy>)'
               -- Example 'var group = (x,y,z).group_by(q,t)'
               -- We also support expressions that both group and reduce records, e.g.:
               -- `(var minx, var miny) = (x,y).group_by(q).arg_min(|t|t.0)`.  These get
               -- desugared into a pair of 'RHSGroupBy' and 'RHSCondition' clauses:
               -- `var __group = (x,y).group_by(q),
               --  (var minx, var miny) = __group.arg_min(|t|t.0)`.
             | RHSGroupBy {
                   -- The resulting group of type 'Group<K,V>', where 'K' is the type of 'rhsGroupBy'
                   -- and 'V' is the type of 'rhsProject'.
                   rhsVar :: String,
                   -- Maps a record in the input collection into a value to store in the group.
                   -- This can be arbitrary expression over variables visible right before the 'group_by'
                   -- clause.
                   rhsProject :: Expr,
                   -- Group-by expression must be a tuple consisting of variable names, e.g., `group_by(q)`, `group_by(p,q,r)`.
                   rhsGroupBy :: Expr
               }
             | RHSFlatMap   {rhsVars :: Expr, rhsMapExpr :: Expr}
             | RHSInspect   {rhsInspectExpr :: Expr}
             deriving (Eq, Ord)

instance PP RuleRHS where
    pp (RHSLiteral True a)    = pp a
    pp (RHSLiteral False a)   = "not" <+> pp a
    pp (RHSCondition c)       = pp c
    pp (RHSGroupBy v p gb)    = "var" <+> pp v <+> "=" <+> pp p <> ".group_by(" <> pp gb <> ")"
    pp (RHSFlatMap v e)       = pp v <+> "=" <+> "FlatMap" <> (parens $ pp e)
    pp (RHSInspect e)         = "Inspect" <+> pp e

instance Show RuleRHS where
    show = render . pp

rhsIsLiteral :: RuleRHS -> Bool
rhsIsLiteral RHSLiteral{} = True
rhsIsLiteral _            = False

rhsIsPositiveLiteral :: RuleRHS -> Bool
rhsIsPositiveLiteral RHSLiteral{..} | rhsPolarity = True
rhsIsPositiveLiteral _                            = False

rhsIsNegativeLiteral :: RuleRHS -> Bool
rhsIsNegativeLiteral RHSLiteral{..} | not rhsPolarity = True
rhsIsNegativeLiteral _                                = False

rhsIsGroupBy :: RuleRHS -> Bool
rhsIsGroupBy RHSGroupBy{} = True
rhsIsGroupBy _            = False

rhsIsCondition :: RuleRHS -> Bool
rhsIsCondition RHSCondition{} = True
rhsIsCondition _              = False

rhsIsFilterCondition :: RuleRHS -> Bool
rhsIsFilterCondition (RHSCondition (E ESet{})) = False
rhsIsFilterCondition (RHSCondition _)          = True
rhsIsFilterCondition _                         = False

rhsIsAssignment :: RuleRHS -> Bool
rhsIsAssignment rhs = rhsIsCondition rhs && not (rhsIsFilterCondition rhs)

data Rule = Rule { rulePos      :: Pos
                 , ruleModule   :: ModuleName
                 , ruleLHS      :: [RuleLHS]
                 , ruleRHS      :: [RuleRHS]
                 }

instance Eq Rule where
    (==) (Rule _ m1 lhs1 rhs1) (Rule _ m2 lhs2 rhs2) =
        m1 == m2 && lhs1 == lhs2 && rhs1 == rhs2

instance Ord Rule where
    compare (Rule _ m1 lhs1 rhs1) (Rule _ m2 lhs2 rhs2) =
        compare (m1, lhs1, rhs1) (m2, lhs2, rhs2)

instance WithPos Rule where
    pos = rulePos
    atPos r p = r{rulePos = p}

instance PP Rule where
    pp Rule{..} = (vcommaSep $ map pp ruleLHS) <+>
                  (if null ruleRHS
                      then empty
                      else ":-" <+> (commaSep $ map pp ruleRHS)) <> "."

instance Show Rule where
    show = render . pp

data ExprNode e = EVar          {exprPos :: Pos, exprVar :: String}
                  -- Call function of closure.
                | EApply        {exprPos :: Pos, exprFunc :: e, exprArgs :: [e]}
                | EField        {exprPos :: Pos, exprStruct :: e, exprField :: String}
                | ETupField     {exprPos :: Pos, exprTuple :: e, exprTupField :: Int}
                | EBool         {exprPos :: Pos, exprBVal :: Bool}
                | EInt          {exprPos :: Pos, exprIVal :: Integer}
                | EFloat        {exprPos :: Pos, exprFVal :: Float}
                | EDouble       {exprPos :: Pos, exprDVal :: Double}
                | EString       {exprPos :: Pos, exprString :: String}
                | EBit          {exprPos :: Pos, exprWidth :: Int, exprIVal :: Integer}
                | ESigned       {exprPos :: Pos, exprWidth :: Int, exprIVal :: Integer}
                | EStruct       {exprPos :: Pos, exprConstructor :: String, exprStructFields :: [(IdentifierWithPos, e)]}
                | ETuple        {exprPos :: Pos, exprTupleFields :: [e]}
                | ESlice        {exprPos :: Pos, exprOp :: e, exprH :: Int, exprL :: Int}
                | EMatch        {exprPos :: Pos, exprMatchExpr :: e, exprCases :: [(e, e)]}
                | EVarDecl      {exprPos :: Pos, exprVName :: String}
                | ESeq          {exprPos :: Pos, exprLeft :: e, exprRight :: e}
                | EITE          {exprPos :: Pos, exprCond :: e, exprThen :: e, exprElse :: e}
                | EFor          {exprPos :: Pos, exprLoopVars :: e, exprIter :: e, exprBody :: e}
                | ESet          {exprPos :: Pos, exprLVal :: e, exprRVal :: e}
                | EBreak        {exprPos :: Pos}
                | EContinue     {exprPos :: Pos}
                | EReturn       {exprPos :: Pos, exprRetVal :: e}
                | EBinOp        {exprPos :: Pos, exprBOp :: BOp, exprLeft :: e, exprRight :: e}
                | EUnOp         {exprPos :: Pos, exprUOp :: UOp, exprOp :: e}
                | EPHolder      {exprPos :: Pos}
                | EBinding      {exprPos :: Pos, exprVar :: String, exprPattern :: e}
                | ETyped        {exprPos :: Pos, exprExpr :: e, exprTSpec :: Type}
                | EAs           {exprPos :: Pos, exprExpr :: e, exprTSpec :: Type}
                | ERef          {exprPos :: Pos, exprPattern :: e}
                | ETry          {exprPos :: Pos, exprExpr :: e}
                  -- 'function(x, y) {e}'
                | EClosure      {exprPos :: Pos, exprClosureArgs :: [ClosureExprArg], exprClosureType :: Maybe Type, exprExpr :: e}
                  -- Expression that refers to a function by name.
                  -- After flattening the module hierarchy, a function name can
                  -- refer to functions in one or more modules.  Type inference
                  -- resolves the ambiguity, leaving exactly one.
                | EFunc         {exprPos :: Pos, exprFuncName :: [String]}

-- Argument of a closure expression:
-- * name
-- * optional 'mut' attribute and type
data ClosureExprArg = ClosureExprArg {
    ceargPos  :: Pos,
    ceargName :: String,
    ceargType :: Maybe ArgType
}

instance WithName ClosureExprArg where
    name = ceargName
    setName a n = a {ceargName = n}

instance Eq ClosureExprArg where
    (==) (ClosureExprArg _ n1 t1) (ClosureExprArg _ n2 t2) = (n1, t1) == (n2, t2)

instance Ord ClosureExprArg where
    compare (ClosureExprArg _ n1 t1) (ClosureExprArg _ n2 t2) = compare (n1, t1) (n2, t2)

instance WithPos ClosureExprArg where
    pos = ceargPos
    atPos a p = a{ceargPos = p}

instance PP ClosureExprArg where
    pp ClosureExprArg{..} = pp ceargName <> maybe empty ((":" <+>) . pp) ceargType

instance Eq e => Eq (ExprNode e) where
    (==) (EVar _ v1)              (EVar _ v2)                = v1 == v2
    (==) (EApply _ f1 as1)        (EApply _ f2 as2)          = f1 == f2 && as1 == as2
    (==) (EField _ s1 f1)         (EField _ s2 f2)           = s1 == s2 && f1 == f2
    (==) (ETupField _ s1 f1)      (ETupField _ s2 f2)        = s1 == s2 && f1 == f2
    (==) (EBool _ b1)             (EBool _ b2)               = b1 == b2
    (==) (EInt _ i1)              (EInt _ i2)                = i1 == i2
    (==) (EFloat _ i1)            (EFloat _ i2)              = i1 == i2
    (==) (EDouble _ i1)           (EDouble _ i2)             = i1 == i2
    (==) (EString _ s1)           (EString _ s2)             = s1 == s2
    (==) (EBit _ w1 i1)           (EBit _ w2 i2)             = w1 == w2 && i1 == i2
    (==) (ESigned _ w1 i1)        (ESigned _ w2 i2)          = w1 == w2 && i1 == i2
    (==) (EStruct _ c1 fs1)       (EStruct _ c2 fs2)         = c1 == c2 && fs1 == fs2
    (==) (ETuple _ fs1)           (ETuple _ fs2)             = fs1 == fs2
    (==) (ESlice _ e1 h1 l1)      (ESlice _ e2 h2 l2)        = e1 == e2 && h1 == h2 && l1 == l2
    (==) (EMatch _ e1 cs1)        (EMatch _ e2 cs2)          = e1 == e2 && cs1 == cs2
    (==) (EVarDecl _ v1)          (EVarDecl _ v2)            = v1 == v2
    (==) (ESeq _ l1 r1)           (ESeq _ l2 r2)             = l1 == l2 && r1 == r2
    (==) (EITE _ i1 t1 e1)        (EITE _ i2 t2 e2)          = i1 == i2 && t1 == t2 && e1 == e2
    (==) (EFor _ v1 e1 b1)        (EFor _ v2 e2 b2)          = v1 == v2 && e1 == e2 && b1 == b2
    (==) (ESet _ l1 r1)           (ESet _ l2 r2)             = l1 == l2 && r1 == r2
    (==) (EBreak _)               (EBreak _)                 = True
    (==) (EContinue _)            (EContinue _)              = True
    (==) (EReturn _ e1)           (EReturn _ e2)             = e1 == e2
    (==) (EBinOp _ o1 l1 r1)      (EBinOp _ o2 l2 r2)        = o1 == o2 && l1 == l2 && r1 == r2
    (==) (EUnOp _ o1 e1)          (EUnOp _ o2 e2)            = o1 == o2 && e1 == e2
    (==) (EPHolder _)             (EPHolder _)               = True
    (==) (EBinding _ v1 e1)       (EBinding _ v2 e2)         = v1 == v2 && e1 == e2
    (==) (ETyped _ e1 t1)         (ETyped _ e2 t2)           = e1 == e2 && t1 == t2
    (==) (EAs _ e1 t1)            (EAs _ e2 t2)              = e1 == e2 && t1 == t2
    (==) (ERef _ p1)              (ERef _ p2)                = p1 == p2
    (==) (ETry _ e1)              (ETry _ e2)                = e1 == e2
    (==) (EClosure _ as1 r1 e1)   (EClosure _ as2 r2 e2)     = (as1, r1, e1) == (as2, r2, e2)
    (==) (EFunc _ f1)             (EFunc _ f2)               = f1 == f2
    (==) _                        _                          = False

-- Assign rank to constructors; used in the implementation of Ord.
erank :: ExprNode e -> Int
erank EVar      {} = 0
erank EApply    {} = 1
erank EField    {} = 2
erank ETupField {} = 3
erank EBool     {} = 4
erank EInt      {} = 5
erank EFloat    {} = 6
erank EDouble   {} = 7
erank EString   {} = 8
erank EBit      {} = 9
erank ESigned   {} = 10
erank EStruct   {} = 11
erank ETuple    {} = 12
erank ESlice    {} = 13
erank EMatch    {} = 14
erank EVarDecl  {} = 15
erank ESeq      {} = 16
erank EITE      {} = 17
erank EFor      {} = 18
erank ESet      {} = 19
erank EBreak    {} = 20
erank EContinue {} = 21
erank EReturn   {} = 22
erank EBinOp    {} = 23
erank EUnOp     {} = 24
erank EPHolder  {} = 25
erank EBinding  {} = 26
erank ETyped    {} = 27
erank EAs       {} = 28
erank ERef      {} = 29
erank ETry      {} = 30
erank EClosure  {} = 31
erank EFunc     {} = 32

instance Ord e => Ord (ExprNode e) where
    compare (EVar _ v1)              (EVar _ v2)                = compare v1 v2
    compare (EApply _ f1 as1)        (EApply _ f2 as2)          = compare (f1, as1) (f2, as2)
    compare (EField _ s1 f1)         (EField _ s2 f2)           = compare (s1, f1)  (s2, f2)
    compare (ETupField _ s1 f1)      (ETupField _ s2 f2)        = compare (s1, f1)  (s2, f2)
    compare (EBool _ b1)             (EBool _ b2)               = compare b1 b2
    compare (EInt _ i1)              (EInt _ i2)                = compare i1 i2
    compare (EFloat _ i1)            (EFloat _ i2)              = compare i1 i2
    compare (EDouble _ i1)           (EDouble _ i2)             = compare i1 i2
    compare (EString _ s1)           (EString _ s2)             = compare s1 s2
    compare (EBit _ w1 i1)           (EBit _ w2 i2)             = compare (w1, i1) (w2, i2)
    compare (ESigned _ w1 i1)        (ESigned _ w2 i2)          = compare (w1, i1) (w2, i2)
    compare (EStruct _ c1 fs1)       (EStruct _ c2 fs2)         = compare (c1, fs1) (c2, fs2)
    compare (ETuple _ fs1)           (ETuple _ fs2)             = compare fs1 fs2
    compare (ESlice _ e1 h1 l1)      (ESlice _ e2 h2 l2)        = compare (e1, h1, l1) (e2, h2, l2)
    compare (EMatch _ e1 cs1)        (EMatch _ e2 cs2)          = compare (e1, cs1) (e2, cs2)
    compare (EVarDecl _ v1)          (EVarDecl _ v2)            = compare v1 v2
    compare (ESeq _ l1 r1)           (ESeq _ l2 r2)             = compare (l1, r1) (l2, r2)
    compare (EITE _ i1 t1 e1)        (EITE _ i2 t2 e2)          = compare (i1, t1, e1) (i2, t2, e2)
    compare (EFor _ v1 e1 b1)        (EFor _ v2 e2 b2)          = compare (v1, e1, b1) (v2, e2, b2)
    compare (ESet _ l1 r1)           (ESet _ l2 r2)             = compare (l1, r1) (l2, r2)
    compare (EBreak _)               (EBreak _)                 = EQ
    compare (EContinue _)            (EContinue _)              = EQ
    compare (EReturn _ e1)           (EReturn _ e2)             = compare e1 e2
    compare (EBinOp _ o1 l1 r1)      (EBinOp _ o2 l2 r2)        = compare (o1, l1, r1) (o2, l2, r2)
    compare (EUnOp _ o1 e1)          (EUnOp _ o2 e2)            = compare (o1, e1) (o2, e2)
    compare (EPHolder _)             (EPHolder _)               = EQ
    compare (EBinding _ v1 e1)       (EBinding _ v2 e2)         = compare (v1, e1) (v2, e2)
    compare (ETyped _ e1 t1)         (ETyped _ e2 t2)           = compare (e1, t1) (e2, t2)
    compare (EAs _ e1 t1)            (EAs _ e2 t2)              = compare (e1, t1) (e2, t2)
    compare (ERef _ p1)              (ERef _ p2)                = compare p1 p2
    compare (ETry _ e1)              (ETry _ e2)                = compare e1 e2
    compare (EClosure _ as1 r1 e1)   (EClosure _ as2 r2 e2)     = compare (as1, r1, e1) (as2, r2, e2)
    compare (EFunc _ f1)              (EFunc _ f2)              = compare f1 f2
    compare e1                       e2                         = compare (erank e1) (erank e2)

instance WithPos (ExprNode e) where
    pos = exprPos
    atPos e p = e{exprPos = p}

-- Expressions that do not represent a single `term` must be printed in parens, so that
-- they parse correctly in contexts that require a `term`.
instance PP e => PP (ExprNode e) where
    pp (EVar _ v)            = pp v
    pp (EApply _ e as)       = parens $ pp e <> (parens $ commaSep $ map pp as)
    pp (EField _ s f)        = parens $ pp s <> char '.' <> pp f
    pp (ETupField _ s f)     = parens $ pp s <> char '.' <> pp f
    pp (EBool _ True)        = "true"
    pp (EBool _ False)       = "false"
    pp (EInt _ v)            = pp v
    pp (EFloat _ v)          = "32'f" <> pp v
    pp (EDouble _ v)         = "64'f" <> pp v
    pp (EString _ s) | isInfixOf "${" s
                              = "[|" <> pp s <> "|]"
                     | otherwise
                             = pp $ show s
    pp (EBit _ w v)          = pp w <> "'d" <> pp v
    pp (ESigned _ w v)       = pp w <> "'sd" <> pp v
    pp (EStruct _ s fs)      = pp s <> (braces $ commaSep
                                        $ map (\(n,e) -> (if null (name n) then empty else ("." <> pp n <> "=")) <> pp e) fs)
    pp (ETuple _ fs)         = parens $ commaSep $ map pp fs
    pp (ESlice _ e h l)      = parens $ pp e <> (brackets $ pp h <> colon <> pp l)
    pp (EMatch _ e cs)       = "match" <+> parens (pp e) <+> "{"
                               $$
                               (nest' $ vcommaSep $ (map (\(c,v) -> pp c <+> "->" <+> pp v) cs))
                               $$
                               "}"
    pp (EVarDecl _ v)        = "var" <+> pp v
    pp (ESeq _ l r)          = braces $ (pp l <> semi) $$ pp r
    pp (EITE _ c t e)        = ("if" <+> pp c <+> lbrace)
                               $$
                               (nest' $ pp t)
                               $$
                               rbrace <+> (("else" <+> lbrace) $$ (nest' $ pp e) $$ rbrace)
    pp (EFor _ v e b)        = "for" <+> (parens $ pp v <+> "in" <+> pp e) <+> lbrace $$
                               (nest' $ pp b)                                         $$
                               rbrace
    pp (ESet _ l r)          = parens $ pp l <+> "=" <+> pp r
    pp (EBreak _)            = "break"
    pp (EContinue _)         = "continue"
    pp (EReturn _ e)         = parens $ "return" <+> pp e
    pp (EBinOp _ op e1 e2)   = parens $ pp e1 <+> pp op <+> pp e2
    pp (EUnOp _ op e)        = parens $ pp op <+> pp e
    pp (EPHolder _)          = "_"
    pp (EBinding _ v e)      = parens $ pp v <> "@" <+> pp e
    pp (ETyped _ e t)        = parens $ pp e <> ":" <+> pp t
    pp (EAs _ e t)           = parens $ pp e <+> "as" <+> pp t
    pp (ERef _ e)            = parens $ "&" <> pp e
    pp (ETry _ e)            = parens $ pp e <> "?"
    pp (EClosure _ as r e)   = parens $ "function" <> parens (commaSep $ map pp as) <> (maybe empty ((":" <>) . pp) r) <> (braces $ pp e)
    pp (EFunc _ [f])         = pp f
    pp (EFunc _ fs)          = "[One of [" <> commaSep (map pp fs) <> "]]"


instance PP e => Show (ExprNode e) where
    show = render . pp

type ENode = ExprNode Expr

newtype Expr = E ENode deriving Ord
enode :: Expr -> ExprNode Expr
enode (E n) = n

instance Eq Expr where
    (==) (E e1) (E e2) = e1 == e2

instance PP Expr where
    pp (E n) = pp n

instance Show Expr where
    show (E n) = show n

instance WithPos Expr where
    pos (E n) = pos n
    atPos (E n) p = E $ atPos n p

eVar v              = E $ EVar      nopos v
eTypedVar v t       = eTyped (eVar v) t
eApply f as         = E $ EApply    nopos f as
eApplyFunc f as     = E $ EApply    nopos (eFunc f) as
eField e f          = E $ EField    nopos e f
eTupField e f       = E $ ETupField nopos e f
eBool b             = E $ EBool     nopos b
eTrue               = eBool True
eFalse              = eBool False
eInt i              = E $ EInt      nopos i
eFloat i            = E $ EFloat    nopos i
eDouble i           = E $ EDouble   nopos i
eString s           = E $ EString   nopos s
eBit w v            = E $ EBit      nopos w v
eSigned w v         = E $ ESigned   nopos w v
eStruct c as t      = eTyped (E $ EStruct nopos c as) t
eTuple [a]          = a
eTuple as           = E $ ETuple    nopos as
eSlice e h l        = E $ ESlice    nopos e h l
eMatch e cs         = E $ EMatch    nopos e cs
eVarDecl v t        = eTyped (E $ EVarDecl  nopos v) t
eSeq l r            = E $ ESeq      nopos l r
eITE i t e          = E $ EITE      nopos i t e
eFor v e b          = E $ EFor      nopos v e b
eSet l r            = E $ ESet      nopos l r
eBreak              = E $ EBreak    nopos
eContinue           = E $ EContinue nopos
eReturn e t         = eTyped (E $ EReturn   nopos e) t
eBinOp op l r       = E $ EBinOp    nopos op l r
eUnOp op e          = E $ EUnOp     nopos op e
eNot e              = eUnOp Not e
ePHolder            = E $ EPHolder  nopos
eBinding v e        = E $ EBinding  nopos v e
eTyped e t          = E $ ETyped    nopos e t
eAs e t             = E $ EAs       nopos e t
eRef e              = E $ ERef      nopos e
eTry e              = E $ ETry      nopos e
eClosure ts r e     = E $ EClosure  nopos ts r e
eFunc f             = E $ EFunc     nopos [f]

data FuncArg = FuncArg { argPos   :: Pos
                       , argAttrs :: [Attribute]
                       , argName  :: String
                       , argType  :: ArgType
                       }

instance Eq FuncArg where
    (==) (FuncArg _ a1 n1 t1) (FuncArg _ a2 n2 t2) = (a1, n1, t1) == (a2, n2, t2)

instance Ord FuncArg where
    compare (FuncArg _ a1 n1 t1) (FuncArg _ a2 n2 t2) = compare (a1, n1, t1) (a2, n2, t2)

instance WithPos FuncArg where
    pos = argPos
    atPos a p = a{argPos = p}

instance WithName FuncArg where
    name = argName
    setName a n = a { argName = n }

instance PP FuncArg where
    pp FuncArg{..} = ppAttributes argAttrs <+> pp argName <> ":" <+> pp argType

argMut :: FuncArg -> Bool
argMut = atypeMut . argType

data Function = Function { funcPos   :: Pos
                         , funcAttrs :: [Attribute]
                         , funcName  :: String
                         , funcArgs  :: [FuncArg]
                         , funcType  :: Type
                         , funcDef   :: Maybe Expr
                         }

funcIsExtern :: Function -> Bool
funcIsExtern f = isNothing $ funcDef f

funcMutArgs :: Function -> [FuncArg]
funcMutArgs f = filter argMut $ funcArgs f

funcImmutArgs :: Function -> [FuncArg]
funcImmutArgs f = filter (not . argMut) $ funcArgs f

instance Eq Function where
    (==) (Function _ at1 n1 as1 t1 d1) (Function _ at2 n2 as2 t2 d2) =
        at1 == at2 && n1 == n2 && as1 == as2 && t1 == t2 && d1 == d2

instance Ord Function where
    compare (Function _ at1 n1 as1 t1 d1) (Function _ at2 n2 as2 t2 d2) =
        compare (at1, n1, as1, t1, d1) (at2, n2, as2, t2, d2)

instance WithPos Function where
    pos = funcPos
    atPos f p = f{funcPos = p}

instance WithName Function where
    name = funcName
    setName f n = f{funcName = n}

instance PP Function where
    pp Function{..} = (ppAttributes funcAttrs) $$
                      (maybe "extern" (\_ -> empty) funcDef) <+>
                      ("function" <+> pp funcName
                       <+> (parens $ commaSep $ map pp funcArgs)
                       <> colon <+> pp funcType)
                      $$
                       (maybe empty (braces' . pp) funcDef)

instance Show Function where
    show = render . pp

funcShowProto :: Function -> String
funcShowProto Function{..} = render $
    "function" <+> pp funcName
    <+> (parens $ commaSep $ map pp funcArgs)
    <> colon <+> pp funcType

-- | Type variables used in function declaration in the order they appear in the declaration
funcTypeVars :: Function -> [String]
funcTypeVars Function{..} = nub $ concatMap (typeTypeVars . atypeType . argType) funcArgs ++
                                  typeTypeVars funcType

data ModuleName = ModuleName {modulePath :: [String]}
                  deriving (Eq, Ord)

instance PP ModuleName where
    pp (ModuleName p) = hcat $ punctuate "::" $ map pp p

instance Show ModuleName where
    show = render . pp


-- | Higher-order type (function or relation).
data HOType = HOTypeRelation { hotPos :: Pos, hotType :: Type }
            | HOTypeFunction { hotPos :: Pos, hotArgs :: [FuncArg], hotType :: Type }

instance Eq HOType where
    (==) (HOTypeRelation _ t1)     (HOTypeRelation _ t2)     = t1 == t2
    (==) (HOTypeFunction _ as1 t1) (HOTypeFunction _ as2 t2) = (as1, t1) == (as2, t2)
    (==) _                         _                         = False

instance WithPos HOType where
    pos = hotPos
    atPos t p = t{hotPos = p}

instance PP HOType where
    pp (HOTypeRelation _ t)     = "relation[" <> pp t <> "]"
    pp (HOTypeFunction _ as t)  = "function(" <> (commaSep $ map pp as) <> "): " <> pp t

instance Show HOType where
    show = render . pp

hotypeIsRelation :: HOType -> Bool
hotypeIsRelation HOTypeRelation{} = True
hotypeIsRelation _                = False

hotypeIsFunction :: HOType -> Bool
hotypeIsFunction HOTypeFunction{} = True
hotypeIsFunction _                = False

-- | Type variables used in transformer declaration in the order they appear in the declaration
hotypeTypeVars :: HOType -> [String]
hotypeTypeVars HOTypeRelation{..} = typeTypeVars hotType
hotypeTypeVars HOTypeFunction{..} = nub $
    concatMap (typeTypeVars . atypeType . argType) hotArgs ++
    typeTypeVars hotType

-- | Argument or field of a higher-order type
data HOField = HOField { hofPos  :: Pos
                       , hofName :: String
                       , hofType :: HOType
                       }

instance Eq HOField where
    (==) (HOField _ n1 t1) (HOField _ n2 t2) = (n1, t1) == (n2, t2)

instance WithPos HOField where
    pos = hofPos
    atPos f p = f{hofPos = p}

instance WithName HOField where
    name = hofName
    setName f n = f { hofName = n }

instance PP HOField where
    pp (HOField _ n t) = pp n <> ":" <+> pp t

instance Show HOField where
    show = render . pp

-- | Relation transformer
data Transformer = Transformer{ transPos     :: Pos
                              , transExtern  :: Bool
                              , transName    :: String
                              , transInputs  :: [HOField]
                              , transOutputs :: [HOField]
                              }

instance Eq Transformer where
    (==) (Transformer _ e1 n1 in1 out1) (Transformer _ e2 n2 in2 out2) = (e1, n1, in1, out1) == (e2, n2, in2, out2)

instance WithPos Transformer where
    pos = transPos
    atPos t p = t{transPos = p}

instance WithName Transformer where
    name = transName
    setName t n = t { transName = n }

instance PP Transformer where
    pp (Transformer _ e n inp outp) = (if e then "extern" else empty) <+> "transformer" <+> pp n <>
                                      (parens $ commaSep $ map pp inp) <+> "->" <+>
                                      (parens $ commaSep $ map pp outp)

instance Show Transformer where
    show = render . pp

-- | Type variables used in transformer declaration in the order they appear in the declaration.
transformerTypeVars :: Transformer -> [String]
transformerTypeVars Transformer{..} = nub $
    concatMap (hotypeTypeVars . hofType) $ transInputs ++ transOutputs

-- | Relation transformer instantiation.
data Apply = Apply { applyPos         :: Pos
                   -- Module where the transformer is instantiated.
                   -- (we track it so we can generate code for it in
                   -- the corresponding Rust module.)
                   , applyModule      :: ModuleName
                   , applyTransformer :: String
                   , applyInputs      :: [String]
                   , applyOutputs     :: [String]
                   }

instance Eq Apply where
    (==) (Apply _ m1 t1 i1 o1) (Apply _ m2 t2 i2 o2) = (m1, t1, i1, o1) == (m2, t2, i2, o2)

instance WithPos Apply where
    pos = applyPos
    atPos a p = a{applyPos = p}

instance PP Apply where
    pp (Apply _ _ t i o) = "apply" <+> pp t <> (parens $ commaSep $ map pp i) <+> "->" <+> (parens $ commaSep $ map pp o)

instance Show Apply where
    show = render . pp

-- | Import statement
data Import = Import { importPos    :: Pos
                     , importModule :: ModuleName
                     , importAlias  :: ModuleName
                     }

instance WithPos Import where
    pos = importPos
    atPos i p = i { importPos = p }

instance PP Import where
    pp Import{..} = "import" <+> pp importModule <+>
                    (if null (modulePath importAlias) then empty else "as" <+> pp importAlias)

instance Show Import where
    show = render . pp

instance Eq Import where
    (==) (Import _ p1 a1) (Import _ p2 a2) = p1 == p2 && a1 == a2

data DatalogProgram = DatalogProgram { progImports      :: [Import]
                                     , progTypedefs     :: M.Map String TypeDef
                                       -- There can be multiple functions with
                                       -- the same name.  The key in the map is
                                       -- function name and number of arguments.
                                     , progFunctions    :: M.Map String [Function]
                                     , progTransformers :: M.Map String Transformer
                                     , progRelations    :: M.Map String Relation
                                     , progIndexes      :: M.Map String Index
                                     , progRules        :: [Rule]
                                     , progApplys       :: [Apply]
                                     , progSources      :: M.Map String String -- maps module to source
                                     }
                      deriving (Eq)

instance PP DatalogProgram where
    pp DatalogProgram{..} = vcat $ punctuate "" $
                            ((map pp progImports)
                             ++
                             (map pp $ M.elems progTypedefs)
                             ++
                             (map pp $ concat $ M.elems progFunctions)
                             ++
                             (map pp $ M.elems progTransformers)
                             ++
                             (map pp $ M.elems progRelations)
                             ++
                             (map pp $ M.elems progIndexes)
                             ++
                             (map pp progRules)
                             ++
                             (map pp progApplys)
                             ++
                             ["\n"])

instance Show DatalogProgram where
    show = render . pp

progStructs :: DatalogProgram -> M.Map String TypeDef
progStructs DatalogProgram{..} =
    M.filter ((\case
                Just TStruct{} -> True
                _              -> False) . tdefType)
             progTypedefs

progConstructors :: DatalogProgram -> [Constructor]
progConstructors = concatMap (typeCons . fromJust . tdefType) . M.elems . progStructs

emptyDatalogProgram :: DatalogProgram
emptyDatalogProgram = DatalogProgram { progImports       = []
                                     , progTypedefs      = M.empty
                                     , progFunctions     = M.empty
                                     , progTransformers  = M.empty
                                     , progRelations     = M.empty
                                     , progIndexes       = M.empty
                                     , progRules         = []
                                     , progApplys        = []
                                     , progSources       = M.empty
                                     }

progAddTypedef :: TypeDef -> DatalogProgram -> DatalogProgram
progAddTypedef tdef prog = prog{progTypedefs = M.insert (name tdef) tdef (progTypedefs prog)}

progAddRules :: [Rule] -> DatalogProgram -> DatalogProgram
progAddRules rules prog = prog{progRules = rules ++ (progRules prog)}

progAddRel :: Relation -> DatalogProgram -> DatalogProgram
progAddRel rel prog = prog{progRelations = M.insert (name rel) rel (progRelations prog)}

-- | Expression's syntactic context determines the kinds of
-- expressions that can appear at this location in the Datalog program,
-- expected type of the expression, and variables visible within the
-- given scope.
--
-- Below, 'X' indicates the position of the expression addressed by
-- context.
--
-- Most 'ECtx' constructors take reference to parent expression
-- ('ctxParExpr') and parent context ('ctxPar').
data ECtx = -- | Top-level context. Serves as the root of the context hierarchy.
            -- Expressions cannot appear directly in this context.
            CtxTop
            -- | Function definition: 'function f(...) = {X}'
          | CtxFunc           {ctxFunc::Function}
            -- | Argument to an atom in the left-hand side of a rule:
            -- 'Rel1[X] :- ...'.
            --       ^
            --       \- context points here
            -- 'ctxAtomIdx' is the index of the LHS atom where the
            -- expression appears
          | CtxRuleLAtom          {ctxRule::Rule, ctxHeadIdx::Int}
            -- Location component of a rule head 'Rel1() @X :- ...'.
          | CtxRuleLLocation      {ctxRule::Rule, ctxHeadIdx::Int}
            -- | Argument to a right-hand-side atom
          | CtxRuleRAtom      {ctxRule::Rule, ctxAtomIdx::Int}
            -- | Filter or assignment expression the RHS of a rule
          | CtxRuleRCond      {ctxRule::Rule, ctxIdx::Int}
            -- | The right-hand side of a FlatMap clause in the RHS of a rule
          | CtxRuleRFlatMap   {ctxRule::Rule, ctxIdx::Int}
            -- | The left-hand side of a FlatMap clause in the RHS of a rule
          | CtxRuleRFlatMapVars{ctxRule::Rule, ctxIdx::Int}
            -- | Inspect clause in the RHS of a rule
          | CtxRuleRInspect   {ctxRule::Rule, ctxIdx::Int}
            -- | Projection expression  in an group_by clause in the RHS of a rule
          | CtxRuleRProject   {ctxRule::Rule, ctxIdx::Int}
            -- | Group-by expression of a group_by clause in the RHS of a rule
          | CtxRuleRGroupBy   {ctxRule::Rule, ctxIdx::Int}
            -- | Key expression
          | CtxKey            {ctxRelation::Relation}
            -- | Index expression
          | CtxIndex          {ctxIndex::Index}
            -- | Argument of a function call
          | CtxApplyArg       {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | Function or closure being invoked.
          | CtxApplyFunc      {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Field expression: 'X.f'
          | CtxField          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Tuple field expression: 'X.N'
          | CtxTupField       {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument passed to a type constructor: 'Cons(X, y, z)'
          | CtxStruct         {ctxParExpr::ENode, ctxPar::ECtx, ctxArg::(Int, IdentifierWithPos)}
            -- | Argument passed to a tuple expression: '(X, y, z)'
          | CtxTuple          {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | Bit slice: 'X[h:l]'
          | CtxSlice          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a match expression: 'match (X) {...}'
          | CtxMatchExpr      {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Match pattern: 'match (...) {X: e1, ...}'
          | CtxMatchPat       {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | Value returned by a match clause: 'match (...) {p1: X, ...}'
          | CtxMatchVal       {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | First expression in a sequence 'X; y'
          | CtxSeq1           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Second expression in a sequence 'y; X'
          | CtxSeq2           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'if (X) ... else ...'
          | CtxITEIf          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'if (cond) X else ...'
          | CtxITEThen        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'if (cond) ... else X'
          | CtxITEElse        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'for (X in ..)'
          | CtxForVars        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'for (.. in e)'
          | CtxForIter        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'for (.. in ..) e'
          | CtxForBody        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Left-hand side of an assignment: 'X = y'
          | CtxSetL           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Righ-hand side of an assignment: 'y = X'
          | CtxSetR           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a 'return' statement
          | CtxReturn         {ctxParExpr::ENode, ctxPar::ECtx}
            -- | First operand of a binary operator: 'X op y'
          | CtxBinOpL         {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Second operand of a binary operator: 'y op X'
          | CtxBinOpR         {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Operand of a unary operator: 'op X'
          | CtxUnOp           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Pattern of a @-expression 'v@pat'
          | CtxBinding        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a typed expression 'X: t'
          | CtxTyped          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a type cast expression 'X as t'
          | CtxAs             {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a &-pattern '&e'
          | CtxRef            {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a ?-expression 'e?'
          | CtxTry            {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Expression inside a closure
          | CtxClosure        {ctxParExpr::ENode, ctxPar::ECtx}
          deriving (Eq, Ord)

instance PP ECtx where
    pp CtxTop        = "CtxTop"
    pp ctx = (pp $ ctxParent ctx) $$ ctx'
        where
        epar = short $ ctxParExpr ctx
        rule = short $ pp $ ctxRule ctx
        rel  = short $ pp $ ctxRelation ctx
        mlen = 100
        short :: (PP a) => a -> Doc
        short = pp . (\x -> if length x < mlen then x else take (mlen - 3) x ++ "...") . (map (\c -> if c == '\n' then ' ' else c)) . render . pp
        ctx' = case ctx of
                    CtxRuleLAtom{..}        -> "CtxRuleLAtom      " <+> rule <+> pp ctxHeadIdx
                    CtxRuleLLocation{..}    -> "CtxRuleLLocation  " <+> rule <+> pp ctxHeadIdx
                    CtxRuleRAtom{..}        -> "CtxRuleRAtom      " <+> rule <+> pp ctxAtomIdx
                    CtxRuleRCond{..}        -> "CtxRuleRCond      " <+> rule <+> pp ctxIdx
                    CtxRuleRFlatMap{..}     -> "CtxRuleRFlatMap   " <+> rule <+> pp ctxIdx
                    CtxRuleRFlatMapVars{..} -> "CtxRuleRFlatMapVars"<+> rule <+> pp ctxIdx
                    CtxRuleRInspect{..}     -> "CtxRuleRInspect   " <+> rule <+> pp ctxIdx
                    CtxRuleRProject{..}     -> "CtxRuleRProject   " <+> rule <+> pp ctxIdx
                    CtxRuleRGroupBy{..}     -> "CtxRuleRGroupBy   " <+> rule <+> pp ctxIdx
                    CtxKey{}                -> "CtxKey            " <+> rel
                    CtxIndex{..}            -> "CtxIndex          " <+> pp (name ctxIndex)
                    CtxFunc{..}             -> "CtxFunc           " <+> (pp $ name ctxFunc)
                    CtxApplyArg{..}         -> "CtxApplyArg       " <+> epar <+> pp ctxIdx
                    CtxApplyFunc{}          -> "CtxApplyFunc      " <+> epar
                    CtxField{}              -> "CtxField          " <+> epar
                    CtxTupField{}           -> "CtxTupField       " <+> epar
                    CtxStruct{..}           -> "CtxStruct         " <+> epar <+> pp (snd ctxArg)
                    CtxTuple{..}            -> "CtxTuple          " <+> epar <+> pp ctxIdx
                    CtxSlice{}              -> "CtxSlice          " <+> epar
                    CtxMatchExpr{}          -> "CtxMatchExpr      " <+> epar
                    CtxMatchPat{..}         -> "CtxMatchPat       " <+> epar <+> pp ctxIdx
                    CtxMatchVal{..}         -> "CtxMatchVal       " <+> epar <+> pp ctxIdx
                    CtxSeq1{}               -> "CtxSeq1           " <+> epar
                    CtxSeq2{}               -> "CtxSeq2           " <+> epar
                    CtxITEIf{}              -> "CtxITEIf          " <+> epar
                    CtxITEThen{}            -> "CtxITEThen        " <+> epar
                    CtxITEElse{}            -> "CtxITEElse        " <+> epar
                    CtxForVars{}            -> "CtxForVars        " <+> epar
                    CtxForIter{}            -> "CtxForIter        " <+> epar
                    CtxForBody{}            -> "CtxForBody        " <+> epar
                    CtxSetL{}               -> "CtxSetL           " <+> epar
                    CtxSetR{}               -> "CtxSetR           " <+> epar
                    CtxReturn{}             -> "CtxReturn         " <+> epar
                    CtxBinOpL{}             -> "CtxBinOpL         " <+> epar
                    CtxBinOpR{}             -> "CtxBinOpR         " <+> epar
                    CtxUnOp{}               -> "CtxUnOp           " <+> epar
                    CtxBinding{}            -> "CtxBinding        " <+> epar
                    CtxTyped{}              -> "CtxTyped          " <+> epar
                    CtxAs{}                 -> "CtxAs             " <+> epar
                    CtxRef{}                -> "CtxRef            " <+> epar
                    CtxTry{}                -> "CtxTry            " <+> epar
                    CtxClosure{}            -> "CtxClosure        " <+> epar
                    CtxTop                  -> error "pp CtxTop"

instance Show ECtx where
    show = render . pp

ctxParent :: ECtx -> ECtx
ctxParent CtxRuleLAtom{}        = CtxTop
ctxParent CtxRuleLLocation{}    = CtxTop
ctxParent CtxRuleRAtom{}        = CtxTop
ctxParent CtxRuleRCond{}        = CtxTop
ctxParent CtxRuleRFlatMap{}     = CtxTop
ctxParent CtxRuleRFlatMapVars{} = CtxTop
ctxParent CtxRuleRInspect{}     = CtxTop
ctxParent CtxRuleRProject{}     = CtxTop
ctxParent CtxRuleRGroupBy{}     = CtxTop
ctxParent CtxKey{}              = CtxTop
ctxParent CtxIndex{}            = CtxTop
ctxParent CtxFunc{}             = CtxTop
ctxParent ctx                   = ctxPar ctx
